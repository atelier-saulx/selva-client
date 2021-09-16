#include <stdio.h>
#include <stddef.h>
#include "redismodule.h"
#include "config.h"
#include "base64.h"
#include "bitmap.h"
#include "errors.h"
#include "hierarchy.h"
#include "selva.h"
#include "selva_object.h"
#include "selva_onload.h"
#include "selva_set.h"
#include "find_index.h"

static Selva_SubscriptionId find_index_sub_id; /* zeroes */
static struct bitmap *find_marker_id_stack;

/**
 * Indexing hint accounting and indices.
 */
struct SelvaFindIndexControlBlock {
    struct {
        unsigned is_valid_marked_id : 1;
        unsigned is_valid_timer_id : 1;
        /**
         * Indexing is active.
         * The index res set is actively being updated although res can be invalid
         * from time to time, meaning that the index is currently invalid.
         */
        unsigned is_active : 1;
        /**
         * res set is considered valid.
         */
        unsigned is_valid : 1;
    };

    /**
     * Subscription marker updating the index.
     */
    Selva_SubscriptionMarkerId marker_id;

    /**
     * Timer refreshing this index control block.
     */
    RedisModuleTimerID timer_id;
    struct {
        size_t take_max;
        size_t tot_max;
        size_t ind_take_max;
    } find_acc;

    /**
     * LFU counter.
     * See LFU_COUNT_xxx macros.
     */
    int lfu_count;

    /*
     * Traversal.
     */
    Selva_NodeId node_id;
    enum SelvaTraversal dir;
    union {
        RedisModuleString *dir_field;
        RedisModuleString *dir_expression;
    };
    RedisModuleString *filter;
    /*
     * End of Traversal.
     */

    RedisModuleString *name; /* name in the index for reverse lookup. */

    /**
     * Result set of the indexing clause.
     * This can be NULL even when we are indexing if it needs to be refreshed
     * after a SELVA_SUBSCRIPTION_FLAG_CL_HIERARCHY was received.
     */
    struct SelvaSet res;
};

static void create_index_cb_timer(RedisModuleCtx *ctx, struct SelvaFindIndexControlBlock *icb);

static size_t calc_name_len(enum SelvaTraversal dir, size_t dir_expression_len, const Selva_NodeId node_id, size_t filter_len) {
    size_t n;

    n = Selva_NodeIdLen(node_id) + base64_out_len(filter_len, 0) + 3;

    if (dir == SELVA_HIERARCHY_TRAVERSAL_BFS_EXPRESSION) {
        /*
         * Currently only expressions are supported in addition to fixed
         * field name traversals.
         */
        n += base64_out_len(dir_expression_len, 0) + 1;
    }

    return n;
}

/**
 * Create a deterministic name for an index.
 * node_id.<direction>[.<dir expression>].H(<indexing clause>) = { lfu_count, SelvaSet<node_id> }
 */
static RedisModuleString *build_name(
        const Selva_NodeId node_id,
        enum SelvaTraversal dir, const char *dir_expression_str, size_t dir_expression_len,
        const char *filter_str, size_t filter_len) {
    size_t name_len = calc_name_len(dir, dir_expression_len, node_id, filter_len);
    char name_str[name_len + 1];
    char *s = name_str;

    /* node_id */
    memcpy(s, node_id, Selva_NodeIdLen(node_id));
    s += Selva_NodeIdLen(node_id);

    /* direction */
    *s++ = '.';
    *s++ = 'A' + (char)__builtin_ffs(dir);
    *s++ = '.';

    if (dir == SELVA_HIERARCHY_TRAVERSAL_BFS_EXPRESSION && dir_expression_len > 0) {
        s += base64_encode_s(s, dir_expression_str, dir_expression_len, 0);
        *s++ = '.';
    }

    /* indexing clause filter */
    s += base64_encode_s(s, filter_str, filter_len, 0);
    *s = '\0';

    return RedisModule_CreateString(NULL, name_str, name_len);
}

static int set_marker_id(struct SelvaFindIndexControlBlock *icb) {
    int next = bitmap_ffs(find_marker_id_stack);

    if (next < 0) {
        return SELVA_ENOBUFS;
    }

    icb->marker_id = next;
    bitmap_clear(find_marker_id_stack, next);

    icb->is_valid_marked_id = 1;

    return 0;
}

/**
 * Check if this node needs to be skipped.
 * This is functionally equivalent to the skipping happening in the find
 * command, meaning that the resulting index set will look similar to a
 * find result with the same arguments.
 */
static int skip_node(const struct SelvaFindIndexControlBlock *icb, const Selva_NodeId node_id) {
    return SelvaTraversal_GetSkip(icb->dir) && !memcmp(node_id, icb->node_id, SELVA_NODE_ID_SIZE);
}

static void update_index(
        RedisModuleCtx *ctx,
        struct SelvaModify_Hierarchy *hierarchy __unused,
        struct Selva_SubscriptionMarker *marker,
        unsigned short event_flags,
        const struct SelvaModify_HierarchyNode *node) {
    struct SelvaFindIndexControlBlock *icb;

    /*
     * Presumably as long as this function is called the owner_ctx pointer
     * should be always point to a valid icb too.
     */
    icb = (struct SelvaFindIndexControlBlock *)marker->marker_action_owner_ctx;

    if (event_flags & SELVA_SUBSCRIPTION_FLAG_CL_HIERARCHY) {
        /* Delete the res to trigger a refresh. */
        if (icb->is_valid) {
#if 0
            Selva_NodeId node_id;

            SelvaHierarchy_GetNodeId(node_id, node);
            fprintf(stderr, "%s:%d: The index must be purged and refreshed because %.*s was removed\n",
                    __FILE__, __LINE__,
                    (int)SELVA_NODE_ID_SIZE, node_id);
#endif

            SelvaSet_Destroy(&icb->res);
            icb->is_valid = 0;

            /* Clear the accounting. */
            memset(&icb->find_acc, 0, sizeof(icb->find_acc));
        }
    } else if (event_flags & SELVA_SUBSCRIPTION_FLAG_REFRESH) {
        /*
         * Presumably there is no way a command would be handled before we have
         * received an event for every node in the traversal, therefore there
         * is no risk setting this valid before all the ids have been
         * actually added.
         */
        if (!icb->is_valid) {
            icb->is_valid = 1;

            /* Initialize the res set before indexing. */
            SelvaSet_Init(&icb->res, SELVA_SET_TYPE_NODEID);
        }

        if (Selva_SubscriptionFilterMatch(ctx, node, marker)) {
            Selva_NodeId node_id;

            SelvaHierarchy_GetNodeId(node_id, node);
#if 0
            fprintf(stderr, "%s:%d: Adding node %.*s to the index after refresh\n",
                    __FILE__, __LINE__,
                    (int)SELVA_NODE_ID_SIZE, node_id);
#endif
            if (!skip_node(icb, node_id)) {
                SelvaSet_Add(&icb->res, node_id);
            }
        }
    } else if (event_flags & (SELVA_SUBSCRIPTION_FLAG_CH_HIERARCHY | SELVA_SUBSCRIPTION_FLAG_CH_FIELD)) {
        /*
         * Note that SELVA_SUBSCRIPTION_FLAG_CH_HIERARCHY applies to both
         * deleting and adding a node. However, we know that currently deleting
         * a node will cause also a SELVA_SUBSCRIPTION_FLAG_CL_HIERARCHY event.
         */
        if (icb->is_valid && Selva_SubscriptionFilterMatch(ctx, node, marker)) {
            Selva_NodeId node_id;

            SelvaHierarchy_GetNodeId(node_id, node);
#if 0
            fprintf(stderr, "%s:%d: Adding node %.*s to the index\n",
                    __FILE__, __LINE__,
                    (int)SELVA_NODE_ID_SIZE, node_id);
#endif
            if (!skip_node(icb, node_id)) {
                SelvaSet_Add(&icb->res, node_id);
            }
        }
    } else {
#if 0
        Selva_NodeId node_id;

        SelvaHierarchy_GetNodeId(node_id, node);
        fprintf(stderr, "%s:%d: NOP %x for node %.*s\n",
                __FILE__, __LINE__,
                (unsigned)event_flags, (int)SELVA_NODE_ID_SIZE, node_id);
#endif
    }
}

/**
 * Start an index.
 */
static int start_index(
        struct SelvaModify_Hierarchy *hierarchy,
        struct SelvaFindIndexControlBlock *icb) {
    const unsigned short marker_flags = SELVA_SUBSCRIPTION_FLAG_CH_HIERARCHY | SELVA_SUBSCRIPTION_FLAG_CH_FIELD | SELVA_SUBSCRIPTION_FLAG_REFRESH;
    const char *dir_field = NULL;
    const char *dir_expression = NULL;
    int err;

    if (icb->dir == SELVA_HIERARCHY_TRAVERSAL_BFS_EXPRESSION) {
        dir_expression = RedisModule_StringPtrLen(icb->dir_expression, NULL);
    } else if (icb->dir_field) {
        dir_field = RedisModule_StringPtrLen(icb->dir_field, NULL);
    }

    err = SelvaSubscriptions_AddCallbackMarker(
            hierarchy, find_index_sub_id, icb->marker_id, marker_flags,
            icb->node_id, icb->dir, dir_field, dir_expression, RedisModule_StringPtrLen(icb->filter, NULL),
            update_index,
            icb);
    if (err) {
        return err;
    }

    icb->is_active = 1;

    /* Clear indexed find accounting. */
    icb->find_acc.ind_take_max = 0;

    return 0;
}

/**
 * Destroy an index.
 */
static int discard_index(
        struct RedisModuleCtx *ctx,
        struct SelvaModify_Hierarchy *hierarchy,
        struct SelvaFindIndexControlBlock *icb) {
    int err;

    if (icb->is_valid_marked_id) {
        err = SelvaSubscriptions_DeleteMarker(ctx, hierarchy, find_index_sub_id, icb->marker_id);
        if (err && err != SELVA_ENOENT && err != SELVA_SUBSCRIPTIONS_ENOENT) {
            return err;
        }
    }

    if (icb->is_valid) {
        /* Destroy the index but not the control block. */
        icb->is_active = 0;
        icb->is_valid = 0;
        SelvaSet_Destroy(&icb->res);
    }

    /* Clear accouting. */
    memset(&icb->find_acc, 0, sizeof(icb->find_acc));

    return 0;
}

/**
 * Destroy index control block completely.
 */
static int destroy_index_cb(
        RedisModuleCtx *ctx,
        struct SelvaModify_Hierarchy *hierarchy,
        struct SelvaFindIndexControlBlock *icb) {
    int err;

#if 0
    fprintf(stderr, "Destroying icb for %s\n", RedisModule_StringPtrLen(icb->name, NULL));
#endif

    if (hierarchy) {
        err = discard_index(ctx, hierarchy, icb);
        if (err) {
            fprintf(stderr, "%s:%d: Failed to discard an index for \"%s\": %s\n",
                    __FILE__, __LINE__,
                    RedisModule_StringPtrLen(icb->name, NULL),
                    getSelvaErrorStr(err));
            return err;
        }
    }

    if (hierarchy && hierarchy->dyn_index) {
        err = SelvaObject_DelKey(hierarchy->dyn_index, icb->name);
        if (err) {
            fprintf(stderr, "%s:%d: Failed to destroy an index for \"%s\": %s",
                    __FILE__, __LINE__,
                    RedisModule_StringPtrLen(icb->name, NULL),
                    getSelvaErrorStr(err));
            return err;
        }
    }

    if (icb->is_valid_marked_id) {
        bitmap_set(find_marker_id_stack, icb->marker_id);
    }

    if (icb->is_valid_timer_id) {
        RedisModule_StopTimer(ctx, icb->timer_id, NULL);
    }

    if (icb->dir == SELVA_HIERARCHY_TRAVERSAL_BFS_EXPRESSION && icb->dir_expression) {
        RedisModule_FreeString(ctx, icb->dir_expression);
    } else if (icb->dir_field) {
        /* Practically not used at the moment. */
        RedisModule_FreeString(ctx, icb->dir_field);
    }

    if (icb->filter) {
        RedisModule_FreeString(ctx, icb->filter);
    }

    if (icb->name) {
        RedisModule_FreeString(NULL, icb->name);
    }

    memset(icb, 0, sizeof(*icb));

    return 0;
}

static void refresh_index_proc(RedisModuleCtx *ctx, void *data) {
    struct SelvaFindIndexControlBlock *icb = (struct SelvaFindIndexControlBlock *)data;
    SelvaModify_Hierarchy *hierarchy;
    int lfu_count;

    icb->is_valid_timer_id = 0;

    hierarchy = SelvaModify_OpenHierarchy(ctx,
                                          RedisModule_CreateString(ctx, HIERARCHY_DEFAULT_KEY, sizeof(HIERARCHY_DEFAULT_KEY) - 1),
                                          REDISMODULE_READ | REDISMODULE_WRITE);
    if (!hierarchy) {
        fprintf(stderr, "%s:%d: Hierarchy not found, not registering a new timer\n",
                __FILE__, __LINE__);
        return;
    }

    /* Recreate the timer. */
    create_index_cb_timer(ctx, icb);

    lfu_count = --icb->lfu_count;
    if (lfu_count <= FIND_LFU_COUNT_DESTROY) {
        int err;

        /*
         * Currently ICBs are mostly destroyed lazily by this mechanism and
         * specifically never when a node is deleted. This is fairly ok
         * because the overhead of this system is minimal and much smaller
         * than actively checking if a deleted node was a part of an ICB.
         */

        err = destroy_index_cb(ctx, hierarchy, icb);
        if (err) {
            fprintf(stderr, "%s:%d: Failed to destroy the index for \"%s\": %s\n",
                    __FILE__, __LINE__,
                    RedisModule_StringPtrLen(icb->name, NULL),
                    getSelvaErrorStr(err));
        }
    } else if (selva_glob_config.find_lfu_count_discard != 0 &&
               lfu_count <= selva_glob_config.find_lfu_count_discard) {
        int err;

        err = discard_index(ctx, hierarchy, icb);
        if (err) {
            fprintf(stderr, "%s:%d: Failed to discard the index for \"%s\": %s\n",
                    __FILE__, __LINE__,
                    RedisModule_StringPtrLen(icb->name, NULL),
                    getSelvaErrorStr(err));
        }
    } else if (icb->find_acc.tot_max >= FIND_INDEXING_THRESHOLD &&
               lfu_count >= selva_glob_config.find_lfu_count_create &&
               !icb->is_valid) {
        int err;

        /*
         * Only call start_index() if the index is not currently valid.
         * Otherwise just a refresh is needed.
         */
        if (!icb->is_active) {
            const RedisModuleString *name = icb->name;
            TO_STR(name);

            err = start_index(hierarchy, icb);
            if (err) {
                fprintf(stderr, "%s:%d: Failed to create an index for \"%.*s\": %s\n",
                        __FILE__, __LINE__,
                        (int)name_len, name_str,
                        getSelvaErrorStr(err));
            } else {
                fprintf(stderr, "%s:%d: Created an index for \"%.*s\"\n",
                       __FILE__, __LINE__,
                      (int)name_len, name_str);
            }
        }

        /*
         * Since we are using the SELVA_SUBSCRIPTION_FLAG_REFRESH flag the call to
         * refresh will call the action function for each node and thus build the
         * initial index.
         */
        err = SelvaSubscriptions_RefreshByMarkerId(ctx, hierarchy, find_index_sub_id, icb->marker_id);
        if (err) {
            fprintf(stderr, "%s:%d: Failed to refresh the index for \"%s\": %s\n",
                    __FILE__, __LINE__,
                    RedisModule_StringPtrLen(icb->name, NULL),
                    getSelvaErrorStr(err));
            /* Get it cleaned up eventually. */
            icb->lfu_count = -1;
            return;
        }
    }
}

/**
 * Get or create an indexing control block.
 */
static struct SelvaFindIndexControlBlock *upsert_index_cb(
        RedisModuleCtx *ctx,
        struct SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId node_id,
        enum SelvaTraversal dir,
        RedisModuleString *dir_expression,
        RedisModuleString *filter) {
    struct SelvaObject *dyn_index = hierarchy->dyn_index;
    TO_STR(dir_expression);
    TO_STR(filter);
    RedisModuleString *name;
    void *p;
    struct SelvaFindIndexControlBlock *icb;
    int err;

    /*
     * Get a deterministic name for indexing this find query.
     */
    name = build_name(node_id, dir, dir_expression_str, dir_expression_len, filter_str, filter_len);
    if (!name) {
        return NULL;
    }

    err = SelvaObject_GetPointer(dyn_index, name, &p);
    icb = p;
    if (err) {
        if (err != SELVA_ENOENT) {
            fprintf(stderr, "%s:%d: Get ICB for \"%s\" failed: %s\n",
                    __FILE__, __LINE__,
                    RedisModule_StringPtrLen(name, NULL),
                    getSelvaErrorStr(err));

            RedisModule_FreeString(NULL, name);
            return NULL;
        }

        /*
         * Create it.
         * This doesn't mean that we are necessarily going to create the index
         * yet but we are going to start counting wether it makes sense to start
         * indexing.
         */
        icb = RedisModule_Calloc(1, sizeof(*icb));
        if (!icb) {
            RedisModule_FreeString(NULL, name);
            return NULL;
        }

        icb->name = name;

        if (set_marker_id(icb)) {
            fprintf(stderr, "%s:%d: Failed to get a marker id for an index \"%s\": %s\n",
                    __FILE__, __LINE__,
                    RedisModule_StringPtrLen(icb->name, NULL),
                    getSelvaErrorStr(err));

            destroy_index_cb(ctx, NULL, icb);
            return NULL;
        }

        icb->lfu_count = selva_glob_config.find_lfu_count_init;

        /*
         * Set traversal params.
         */
        memcpy(icb->node_id, node_id, SELVA_NODE_ID_SIZE);
        icb->dir = dir;

        /* TODO Can it ever be dir_field, currently no... */
        if (dir_expression) {
            RedisModule_RetainString(ctx, dir_expression);
        }
        icb->dir_expression = dir_expression;
        RedisModule_RetainString(ctx, filter);
        icb->filter = filter;

        err = SelvaObject_SetPointer(dyn_index, name, icb, NULL);
        if (err) {
            fprintf(stderr, "%s:%d: Failed to insert a new ICB at \"%s\": %s\n",
                    __FILE__, __LINE__,
                    RedisModule_StringPtrLen(name, NULL),
                    getSelvaErrorStr(err));

            destroy_index_cb(ctx, NULL, icb);
            return NULL;
        }

        /* Finally create a timer. */
        create_index_cb_timer(ctx, icb);
    }

    return icb;
}

static void create_index_cb_timer(RedisModuleCtx *ctx, struct SelvaFindIndexControlBlock *icb) {
    const mstime_t period = FIND_LFU_PERIOD;

    icb->timer_id = RedisModule_CreateTimer(ctx, period, refresh_index_proc, icb);
    icb->is_valid_timer_id = 1;
}

int SelvaFind_AutoIndex(
        RedisModuleCtx *ctx,
        struct SelvaModify_Hierarchy *hierarchy,
        enum SelvaTraversal dir, RedisModuleString *dir_expression,
        const Selva_NodeId node_id,
        RedisModuleString *filter,
        struct SelvaFindIndexControlBlock **icb_out,
        struct SelvaSet **out) {
    struct SelvaFindIndexControlBlock *icb;
    TO_STR(filter);
    filter_str;

    *icb_out = NULL;

    if (!filter || filter_len == 0 || FIND_INDICES_MAX == 0) {
        return SELVA_EINVAL;
    }

    /*
     * Some traversals are never indexed.
     */
    if (dir & (
        SELVA_HIERARCHY_TRAVERSAL_ARRAY | /* (1) */
        SELVA_HIERARCHY_TRAVERSAL_REF | /* (2) */
        SELVA_HIERARCHY_TRAVERSAL_EDGE_FIELD | /* (2) */
        SELVA_HIERARCHY_TRAVERSAL_CHILDREN | /* (2) */
        SELVA_HIERARCHY_TRAVERSAL_PARENTS | /* (2) */
        SELVA_HIERARCHY_TRAVERSAL_DFS_ANCESTORS | /* (3) */
        SELVA_HIERARCHY_TRAVERSAL_DFS_DESCENDANTS | /* (3) */
        SELVA_HIERARCHY_TRAVERSAL_DFS_FULL | /* (3) */
        SELVA_HIERARCHY_TRAVERSAL_EXPRESSION /* (2) */
        )) {
        /*
         * Legends:
         * 1 = the sub marker capabilites are lacking.
         * 2 = technically possible but there is almost no benefit in creating
         *     this index.
         * 3 = unlikely to be used.
         */
        return 0; /* These traversals are not indexed at the moment. */
    }

    icb = upsert_index_cb(ctx, hierarchy, node_id, dir, dir_expression, filter);
    *icb_out = icb;

    if (!icb || !icb->is_valid) {
        return SELVA_ENOENT;
    }

    *out = &icb->res;
    return 0;
}

static void update_lfu_count(struct SelvaFindIndexControlBlock * restrict icb, size_t acc_take) {
    typeof(icb->lfu_count) orig_lfu_count = icb->lfu_count;

    /* Never increment if it's negative. */
    if (orig_lfu_count >= 0) {
        /*
         * Give double increment for the index that was used.
         * TODO This is not accurate because the find command could have
         * actually returned an empty set, in which case the index result set
         * was still actually useful.
         */
        icb->lfu_count += selva_glob_config.find_lfu_count_incr << !!acc_take;

        /* Handle overflow. */
        if (icb->lfu_count < orig_lfu_count) {
            icb->lfu_count = selva_glob_config.find_lfu_count_init;
        }
    }
}

void SelvaFind_Acc(struct SelvaFindIndexControlBlock * restrict icb, size_t acc_take, size_t acc_tot) {
    if (!FIND_INDICES_MAX) {
        /* If indexing is disabled then the rest of the function will be optimized out. */
        return;
    }

    update_lfu_count(icb, acc_take);

    /*
     * Result set size accounting.
     */
    if (icb->is_valid) {
        if (acc_take > icb->find_acc.ind_take_max) {
            icb->find_acc.ind_take_max = acc_take;
        }
    } else {
        if (acc_take > icb->find_acc.take_max || acc_tot > icb->find_acc.tot_max) {
            icb->find_acc.take_max = acc_take;
            icb->find_acc.tot_max = acc_tot;
        }
    }
}

static int list_index(RedisModuleCtx *ctx, struct SelvaObject *obj) {
    SelvaObject_Iterator *it;
    enum SelvaObjectType type;
    void *p;
    int n = 0;

    it = SelvaObject_ForeachBegin(obj);
    while ((p = SelvaObject_ForeachValueType(obj, &it, NULL, &type))) {
        if (type == SELVA_OBJECT_POINTER) {
            const struct SelvaFindIndexControlBlock *icb = (const struct SelvaFindIndexControlBlock *)p;

            n++;
            RedisModule_ReplyWithString(ctx, icb->name);
            RedisModule_ReplyWithArray(ctx, 4);
            RedisModule_ReplyWithLongLong(ctx, icb->find_acc.take_max);
            RedisModule_ReplyWithLongLong(ctx, icb->find_acc.tot_max);
            RedisModule_ReplyWithLongLong(ctx, icb->find_acc.ind_take_max);
            if (!icb->is_active) {
                RedisModule_ReplyWithSimpleString(ctx, "not_active");
            } else if (!icb->is_valid) {
                RedisModule_ReplyWithSimpleString(ctx, "not_valid");
            } else {
                RedisModule_ReplyWithLongLong(ctx, SelvaSet_Size(&icb->res));
            }
        } else if (type == SELVA_OBJECT_OBJECT) {
            n += list_index(ctx, (struct SelvaObject *)p);
        } else {
            fprintf(stderr, "%s:%d: Unsupported index type: %s\n",
                    __FILE__, __LINE__,
                    SelvaObject_Type2String(type, NULL));
        }
    }

    return n;
}

static int SelvaFindIndex_ListCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    SelvaModify_Hierarchy *hierarchy;

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    const int ARGV_REDIS_KEY = 1;

    /*
     * Open the Redis key.
     */
    hierarchy = SelvaModify_OpenHierarchy(ctx, argv[ARGV_REDIS_KEY], REDISMODULE_READ);
    if (!hierarchy) {
        /* Do not send redis messages here. */
        return REDISMODULE_OK;
    }
    if (!hierarchy->dyn_index) {
        return replyWithSelvaError(ctx, SELVA_ENOENT);
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplySetArrayLength(ctx, 2 * list_index(ctx, hierarchy->dyn_index));

    return REDISMODULE_OK;
}

static int SelvaFindIndex_NewCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    SelvaModify_Hierarchy *hierarchy;
    enum SelvaTraversal dir;
    RedisModuleString *dir_expression;
    const RedisModuleString *rms_node_id;
    Selva_NodeId node_id;
    RedisModuleString *filter;
    struct SelvaFindIndexControlBlock *icb = NULL;
    struct SelvaSet *set;
    int err;

    if (argc != 6) {
        return RedisModule_WrongArity(ctx);
    }

    const int ARGV_REDIS_KEY = 1;
    const int ARGV_DIRECTION = 2;
    const int ARGV_REF_FIELD = 3;
    const int ARGV_NODE_ID = 4;
    const int ARGV_FILTER = 5;

    /*
     * Open the Redis key.
     */
    hierarchy = SelvaModify_OpenHierarchy(ctx, argv[ARGV_REDIS_KEY], REDISMODULE_READ);
    if (!hierarchy) {
        /* Do not send redis messages here. */
        return REDISMODULE_OK;
    }
    if (!hierarchy->dyn_index) {
        return replyWithSelvaError(ctx, SELVA_ENOENT);
    }

    err = SelvaTraversal_ParseDir2(&dir, argv[ARGV_DIRECTION]);
    if (err) {
        return replyWithSelvaErrorf(ctx, err, "Traversal argument");
    }

    if (dir == SELVA_HIERARCHY_TRAVERSAL_BFS_EXPRESSION) {
        dir_expression = argv[ARGV_REF_FIELD];
    } else {
        dir_expression = NULL;
    }

    rms_node_id = argv[ARGV_NODE_ID];
    TO_STR(rms_node_id);
    if (rms_node_id_len <= SELVA_NODE_TYPE_SIZE || rms_node_id_len > SELVA_NODE_ID_SIZE) {
        return replyWithSelvaErrorf(ctx, SELVA_EINVAL, "node_id");
    }
    Selva_NodeIdCpy(node_id, rms_node_id_str);

    /* TODO Validate */
    filter = argv[ARGV_FILTER];

    err = SelvaFind_AutoIndex(ctx, hierarchy, dir, dir_expression, node_id, filter, &icb, &set);
    if ((err && err != SELVA_ENOENT) || !icb) {
        return replyWithSelvaErrorf(ctx, err, "Failed to create an index");
    }

    /*
     * Make sure that an index will be created.
     */
    icb->find_acc.tot_max = FIND_INDEXING_THRESHOLD + 1;
    icb->lfu_count = selva_glob_config.find_lfu_count_create + 10;

    return RedisModule_ReplyWithLongLong(ctx, 1);
}

static int SelvaFindIndex_DelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    SelvaModify_Hierarchy *hierarchy;
    void *p;
    struct SelvaFindIndexControlBlock *icb;
    int discard = 0;
    int err;

    const int ARGV_REDIS_KEY = 1;
    const int ARGV_INDEX = 2;
    const int ARGV_OP = 3;

    if (argc == 4) {
        const RedisModuleString *op = argv[ARGV_OP];
        TO_STR(op);

        if (op_len == 1 && op_str[0] == '0') {
            discard = 0;
        } else if (op_len == 1 && op_str[0] == '1') {
            discard = 1;
        } else {
            return replyWithSelvaError(ctx, SELVA_ENOTSUP);
        }
    } else if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    /*
     * Open the Redis key.
     */
    hierarchy = SelvaModify_OpenHierarchy(ctx, argv[ARGV_REDIS_KEY], REDISMODULE_READ);
    if (!hierarchy) {
        /* Do not send redis messages here. */
        return REDISMODULE_OK;
    }
    if (!hierarchy->dyn_index) {
        return replyWithSelvaError(ctx, SELVA_ENOENT);
    }

    err = SelvaObject_GetPointer(hierarchy->dyn_index, argv[ARGV_INDEX], &p);
    icb = p;
    if (err == SELVA_ENOENT) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else if (err) {
        return replyWithSelvaError(ctx, err);
    }

    if (discard) {
        err = discard_index(ctx, hierarchy, icb);
    } else {
        err = destroy_index_cb(ctx, hierarchy, icb);
    }
    if (err) {
        return replyWithSelvaError(ctx, err);
    }

    return RedisModule_ReplyWithLongLong(ctx, 1);
}

static int FindIndex_OnLoad(RedisModuleCtx *ctx __unused) {
    if (FIND_INDICES_MAX == 0) {
        return REDISMODULE_ERR;
    }

    find_marker_id_stack = RedisModule_Alloc(BITMAP_ALLOC_SIZE(FIND_INDICES_MAX));
    if (!find_marker_id_stack) {
        return REDISMODULE_ERR;
    }

    find_marker_id_stack->nbits = FIND_INDICES_MAX;
    for (size_t i = 0; i < FIND_INDICES_MAX; i++) {
        bitmap_set(find_marker_id_stack, i);
    }

    if (RedisModule_CreateCommand(ctx, "selva.index.list", SelvaFindIndex_ListCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR ||
        RedisModule_CreateCommand(ctx, "selva.index.new", SelvaFindIndex_NewCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR ||
        RedisModule_CreateCommand(ctx, "selva.index.del", SelvaFindIndex_DelCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
SELVA_ONLOAD(FindIndex_OnLoad);
