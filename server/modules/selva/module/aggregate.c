#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "selva.h"
#include "redismodule.h"
#include "arg_parser.h"
#include "errors.h"
#include "hierarchy.h"
#include "modify.h"
#include "rpn.h"
#include "selva_node.h"
#include "selva_object.h"
#include "selva_onload.h"
#include "selva_set.h"
#include "subscriptions.h"
#include "svector.h"
#include "traversal.h"

struct AggregateCommand_Args {
    RedisModuleCtx *ctx;
    RedisModuleString *lang;

    RedisModuleString **field_names;
    size_t nr_fields;
    ssize_t nr_results; /*!< Number of results sent. */
};

static int AggregateCommand_NodeCb(struct SelvaModify_HierarchyNode *node, void *arg) {
    Selva_NodeId nodeId;
    RedisModuleKey *key = NULL;
    struct SelvaObject *obj;
    struct AggregateCommand_Args *restrict args = (struct AggregateCommand_Args *)arg;
    int err;

    SelvaModify_HierarchyGetNodeId(nodeId, node);
    err = open_node_key(args->ctx, nodeId, &key, &obj);
    if (err) {
        fprintf(stderr, "%s:%d: Failed to open a node object. nodeId: %.*s error: %s\n",
                __FILE__, __LINE__,
                (int)SELVA_NODE_ID_SIZE, nodeId,
                getSelvaErrorStr(err));
        /* Ignore errors. */
        return 0;
    }

    // TODO

    RedisModule_CloseKey(key);
    return 0;
}

static int AggregateCommand_ArrayNodeCb(struct SelvaObject *obj, void *arg) {
    // TODO
    struct FindCommand_Args *args = (struct FindCommand_Args *)arg;
    struct rpn_ctx *rpn_ctx = args->rpn_ctx;
    int take = (args->offset > 0) ? !args->offset-- : 1;

    if (take && rpn_ctx) {
        int err;

        /* Set obj to the register */
        err = rpn_set_reg_slvobj(rpn_ctx, 0, obj, 0);
        if (err) {
            fprintf(stderr, "%s:%d: Register set failed: \"%s\"\n",
                    __FILE__, __LINE__,
                    rpn_str_error[err]);
            return 1;
        }

        /*
         * Resolve the expression and get the result.
         */
        err = rpn_bool(args->ctx, rpn_ctx, args->filter, &take);
        if (err) {
            fprintf(stderr, "%s:%d: Expression failed: \"%s\"\n",
                    __FILE__, __LINE__,
                    rpn_str_error[err]);
            return 1;
        }
    }

    if (take) {
        const int sort = !!args->order_field;

        if (!sort) {
            ssize_t *nr_nodes = args->nr_nodes;
            ssize_t * restrict limit = args->limit;
            int err;

            // if (args->fields) {
            //     err = send_array_object_fields(args->ctx, args->lang, args->hierarchy, obj, args->fields);
            // } else {
            //     RedisModule_ReplyWithStringBuffer(args->ctx, EMPTY_NODE_ID, SELVA_NODE_ID_SIZE);
            //     err = 0;
            // }
            // if (err) {
            //     RedisModule_ReplyWithNull(args->ctx);
            //     fprintf(stderr, "%s:%d: Failed to handle field(s), err: %s\n",
            //             __FILE__, __LINE__,
            //             getSelvaErrorStr(err));
            // }

            *nr_nodes = *nr_nodes + 1;

            *limit = *limit - 1;
            if (*limit == 0) {
                return 1;
            }
        } else {
            struct FindCommand_OrderedItem *item;
            item = createFindCommand_ObjectBasedOrderItem(args->ctx, args->lang, obj, args->order_field);
            if (item) {
                SVector_InsertFast(args->order_result, item);
            } else {
                /*
                 * It's not so easy to make the response fail at this point.
                 * Given that we shouldn't generally even end up here in real
                 * life, it's fairly ok to just log the error and return what
                 * we can.
                 */
                fprintf(stderr, "%s:%d: Out of memory while creating an ordered result item\n",
                        __FILE__, __LINE__);
            }
        }
    }

    return 0;
}

/**
 * Find node in set.
 * SELVA.inherit REDIS_KEY NODE_ID [TYPE1[TYPE2[...]]] [FIELD_NAME1[ FIELD_NAME2[ ...]]]
 */
int SelvaHierarchy_Aggregate(RedisModuleCtx *ctx, int recursive, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    int err;

    const int ARGV_LANG      = 1;
    const int ARGV_REDIS_KEY = 2;
    const int ARGV_ALGO      = 3;
    const int ARGV_DIRECTION = 4;
    const int ARGV_ORDER_TXT = 5;
    const int ARGV_ORDER_FLD = 6;
    const int ARGV_ORDER_ORD = 7;
    int ARGV_OFFSET_TXT      = 5;
    int ARGV_OFFSET_NUM      = 6;
    int ARGV_LIMIT_TXT       = 5;
    int ARGV_LIMIT_NUM       = 6;
    int ARGV_MERGE_TXT       = 5;
    int ARGV_MERGE_VAL       = 6;
    int ARGV_FIELDS_TXT      = 5;
    int ARGV_FIELDS_VAL      = 6;
    int ARGV_NODE_IDS        = 5;
    int ARGV_FILTER_EXPR     = 6;
    int ARGV_FILTER_ARGS     = 7;
#define SHIFT_ARGS(i) \
    ARGV_OFFSET_TXT += i; \
    ARGV_OFFSET_NUM += i; \
    ARGV_LIMIT_TXT += i; \
    ARGV_LIMIT_NUM += i; \
    ARGV_MERGE_TXT += i; \
    ARGV_MERGE_VAL += i; \
    ARGV_FIELDS_TXT += i; \
    ARGV_FIELDS_VAL += i; \
    ARGV_NODE_IDS += i; \
    ARGV_FILTER_EXPR += i; \
    ARGV_FILTER_ARGS += i

    if (argc < 6) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *lang = argv[ARGV_LANG];

    /*
     * Open the Redis key.
     */
    SelvaModify_Hierarchy *hierarchy = SelvaModify_OpenHierarchy(ctx, argv[ARGV_REDIS_KEY], REDISMODULE_READ);
    if (!hierarchy) {
        return REDISMODULE_OK;
    }

    /*
     * Select traversal method.
     */
    enum SelvaTraversalAlgo algo = HIERARCHY_BFS;
	// TODO?
    // err = parse_algo(&algo, argv[ARGV_ALGO]);
    // if (err) {
    //     return replyWithSelvaErrorf(ctx, err, "traversal method");
    // }

    /*
     * Parse the order arg.
     */
    enum hierarchy_result_order order = HIERARCHY_RESULT_ORDER_NONE;
    const RedisModuleString *order_by_field = NULL;
    if (argc > ARGV_ORDER_ORD) {
        err = parse_order(&order_by_field, &order,
                          argv[ARGV_ORDER_TXT],
                          argv[ARGV_ORDER_FLD],
                          argv[ARGV_ORDER_ORD]);
        if (err == 0) {
            SHIFT_ARGS(3);
        } else if (err != SELVA_HIERARCHY_ENOENT) {
            return replyWithSelvaErrorf(ctx, err, "order");
        }
    }

    /*
     * Parse the offset arg.
     */
    ssize_t offset = 0;
    if (argc > ARGV_OFFSET_NUM) {
        err = SelvaArgParser_IntOpt(&offset, "offset", argv[ARGV_OFFSET_TXT], argv[ARGV_OFFSET_NUM]);
        if (err == 0) {
            SHIFT_ARGS(2);
        } else if (err != SELVA_ENOENT) {
            return replyWithSelvaErrorf(ctx, err, "offset");
        }
        if (offset < -1) {
            return replyWithSelvaErrorf(ctx, err, "offset < -1");
        }
    }

    /*
     * Parse the limit arg. -1 = inf
     */
    ssize_t limit = -1;
    if (argc > ARGV_LIMIT_NUM) {
        err = SelvaArgParser_IntOpt(&limit, "limit", argv[ARGV_LIMIT_TXT], argv[ARGV_LIMIT_NUM]);
        if (err == 0) {
            SHIFT_ARGS(2);
        } else if (err != SELVA_ENOENT) {
            return replyWithSelvaErrorf(ctx, err, "limit");
        }
    }

    /*
     * Parse the merge flag.
     */
    enum merge_strategy merge_strategy = MERGE_STRATEGY_NONE;
    RedisModuleString *merge_path = NULL;
    if (argc > ARGV_MERGE_VAL) {
        err = SelvaArgParser_Enum(merge_types, argv[ARGV_MERGE_TXT]);
        if (err != SELVA_ENOENT) {
            if (err < 0) {
                return replyWithSelvaErrorf(ctx, err, "invalid merge argument");
            }

            if (limit != -1) {
                return replyWithSelvaErrorf(ctx, err, "merge is not supported with limit");
            }

            merge_strategy = err;
            merge_path = argv[ARGV_MERGE_VAL];
            SHIFT_ARGS(2);
        }
    }

    /*
     * Parse fields.
     */
    selvaobject_autofree struct SelvaObject *fields = NULL;
    if (argc > ARGV_FIELDS_VAL) {
		err = SelvaArgsParser_StringSetList(ctx, &fields, "fields", argv[ARGV_FIELDS_TXT], argv[ARGV_FIELDS_VAL]);
        if (err == 0) {
            if (merge_strategy == MERGE_STRATEGY_ALL) {
                /* Having fields set turns a regular merge into a named merge. */
                merge_strategy = MERGE_STRATEGY_NAMED;
            } else if (merge_strategy != MERGE_STRATEGY_NONE) {
                return replyWithSelvaErrorf(ctx, err, "only the regular merge can be used with fields");
            }
            SHIFT_ARGS(2);
        } else if (err != SELVA_ENOENT) {
            return replyWithSelvaErrorf(ctx, err, "fields");
        }
    }
    if (merge_strategy != MERGE_STRATEGY_NONE && (!fields || fields_contains(fields, "*", 1))) {
        if (fields) {
            SelvaObject_Destroy(fields);
        }
        /* Merge needs a fields object anyway but it must be empty. */
        fields = SelvaObject_New();
    }

    struct rpn_ctx *rpn_ctx = NULL;
    struct rpn_expression *filter_expression = NULL;
    struct rpn_ctx *recursive_rpn_ctx = NULL;
    struct rpn_expression *recursive_rpn_expr = NULL;

    /*
     * Prepare the filter expression if given.
     */
    if (argc >= ARGV_FILTER_EXPR + 1) {
        const int nr_reg = argc - ARGV_FILTER_ARGS + 2;
        const char *input;

        rpn_ctx = rpn_init(nr_reg);
        if (!rpn_ctx) {
            return replyWithSelvaErrorf(ctx, SELVA_ENOMEM, "filter expression");
        }

        /*
         * Compile the filter expression.
         */
        input = RedisModule_StringPtrLen(argv[ARGV_FILTER_EXPR], NULL);
        filter_expression = rpn_compile(input);
        if (!filter_expression) {
            rpn_destroy(rpn_ctx);
            return replyWithSelvaErrorf(ctx, SELVA_RPN_ECOMP, "Failed to compile the filter expression");
        }

        /*
         * Get the filter expression arguments and set them to the registers.
         */
        for (int i = ARGV_FILTER_ARGS; i < argc; i++) {
            /* reg[0] is reserved for the current nodeId */
            const size_t reg_i = i - ARGV_FILTER_ARGS + 1;
            size_t str_len;
            const char *str = RedisModule_StringPtrLen(argv[i], &str_len);

            rpn_set_reg(rpn_ctx, reg_i, str, str_len + 1, 0);
        }
    }

    SVECTOR_AUTOFREE(order_result); /*!< for ordered result. */

    if (argc <= ARGV_NODE_IDS) {
        replyWithSelvaError(ctx, SELVA_HIERARCHY_EINVAL);
        goto out;
    }

    RedisModuleString *ids = argv[ARGV_NODE_IDS];
    TO_STR(ids);

    if (order != HIERARCHY_RESULT_ORDER_NONE) {
        if (!SVector_Init(&order_result, (limit > 0) ? limit : HIERARCHY_EXPECTED_RESP_LEN, getOrderFunc(order))) {
            replyWithSelvaError(ctx, SELVA_ENOMEM);
            goto out;
        }
    }

    /*
     * In the recursive mode the direction/field_name field contains
     * an expression that should to evaluate into a set of field names.
     */
    if (recursive) {
        const char *field_selector;

        recursive_rpn_ctx = rpn_init(1);
        if (!recursive_rpn_ctx) {
            replyWithSelvaErrorf(ctx, SELVA_ENOMEM, "field selector expression");
            goto out;
        }

        field_selector = RedisModule_StringPtrLen(argv[ARGV_DIRECTION], NULL);
        recursive_rpn_expr = rpn_compile(field_selector);
        if (!recursive_rpn_expr) {
            rpn_destroy(recursive_rpn_ctx);
            replyWithSelvaErrorf(ctx, SELVA_RPN_ECOMP, "Failed to compile the field selector expression");
            goto out;
        }
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    /*
     * Run for each NODE_ID.
     */
    ssize_t nr_nodes = 0;
    size_t merge_nr_fields = 0;
    const char *array_traversal_ref_field = NULL;
    for (size_t i = 0; i < ids_len; i += SELVA_NODE_ID_SIZE) {
        enum SelvaModify_HierarchyTraversal dir = SELVA_HIERARCHY_TRAVERSAL_NONE;
        Selva_NodeId nodeId;
        RedisModuleString *ref_field = NULL;

        Selva_NodeIdCpy(nodeId, ids_str + i);

        if (!recursive) {
            /*
             * Get the direction parameter.
             */
            err = parse_dir(ctx, hierarchy, &dir, &ref_field, nodeId, algo, argv[ARGV_DIRECTION]);
            if (err) {
                fprintf(stderr, "%s:%d: Error \"%s\" while selecting the field and dir for the node \"%.*s\", skipping\n",
                        __FILE__, __LINE__,
                        getSelvaErrorStr(err),
                        (int)SELVA_NODE_ID_SIZE, nodeId);
                /* Skip this node */
                continue;
            }
        } else {
            /* recursive can use this for get_skip() */
            dir = SELVA_HIERARCHY_TRAVERSAL_BFS_DESCENDANTS;
        }

        /*
         * Run BFS/DFS.
         */
        ssize_t tmp_limit = -1;
        const size_t skip = get_skip(dir); /* Skip n nodes from the results. */
        struct FindCommand_Args args = {
            .ctx = ctx,
            .lang = lang,
            .hierarchy = hierarchy,
            .nr_nodes = &nr_nodes,
            .offset = (order == HIERARCHY_RESULT_ORDER_NONE) ? offset + skip : skip,
            .limit = (order == HIERARCHY_RESULT_ORDER_NONE) ? &limit : &tmp_limit,
            .rpn_ctx = rpn_ctx,
            .filter = filter_expression,
            .merge_strategy = merge_strategy,
            .merge_path = merge_path,
            .merge_nr_fields = &merge_nr_fields,
            .fields = fields,
            .order_field = order_by_field,
            .order_result = &order_result,
        };
        const struct SelvaModify_HierarchyCallback cb = {
            .node_cb = AggregateCommand_NodeCb,
            .node_arg = &args,
        };

        if (limit == 0) {
            break;
        }

        if (recursive) {
            err = SelvaHierarchy_TraverseExpression(ctx, hierarchy, nodeId, recursive_rpn_ctx, recursive_rpn_expr, &cb);
        } else if (dir == SELVA_HIERARCHY_TRAVERSAL_REF && ref_field) {
            TO_STR(ref_field);

            err = SelvaModify_TraverseHierarchyRef(ctx, hierarchy, nodeId, ref_field_str, ref_field_len, &cb);
        } else if (dir == SELVA_HIERARCHY_TRAVERSAL_ARRAY && ref_field) {
            TO_STR(ref_field);
            array_traversal_ref_field = ref_field_str;
            const struct SelvaModify_ArrayObjectCallback ary_cb = {
                .node_cb = AggregateCommand_ArrayNodeCb,
                .node_arg = &args,
            };

            err = SelvaModify_TraverseArray(ctx, hierarchy, nodeId, ref_field_str, ref_field_len, &ary_cb);
        } else if (dir == SELVA_HIERARCHY_TRAVERSAL_BFS_EDGE_FIELD && ref_field) {
            TO_STR(ref_field);

            err = SelvaModify_TraverseHierarchyEdge(hierarchy, nodeId, ref_field_str, ref_field_len, &cb);
        } else {
            err = SelvaModify_TraverseHierarchy(hierarchy, nodeId, dir, &cb);
        }
        if (err != 0) {
            /*
             * We can't send an error to the client at this point so we'll just log
             * it and ignore the error.
             */
            fprintf(stderr, "%s:%d: Find failed for node: \"%.*s\"\n",
                    __FILE__, __LINE__,
                    (int)SELVA_NODE_ID_SIZE, nodeId);
        }
    }

    /*
     * If an ordered request was requested then nothing was send to the client yet
     * and we need to do it now.
     */
	// TODO
    // if (order != HIERARCHY_RESULT_ORDER_NONE) {
    //     nr_nodes = array_traversal_ref_field
    //         ? FindCommand_PrintOrderedArrayResult(ctx, lang, hierarchy, offset, limit, fields, &order_result)
    //         : FindCommand_PrintOrderedResult(ctx, lang, hierarchy, offset, limit, merge_strategy, merge_path, fields, &order_result, &merge_nr_fields);
    // }

    /* nr_nodes is never negative at this point so we can safely cast it. */
    RedisModule_ReplySetArrayLength(ctx, (merge_strategy == MERGE_STRATEGY_NONE) ? (size_t)nr_nodes : merge_nr_fields);

out:
    if (rpn_ctx) {
        rpn_destroy(rpn_ctx);
        rpn_destroy_expression(filter_expression);
    }
    if (recursive_rpn_ctx) {
        rpn_destroy(recursive_rpn_ctx);
        rpn_destroy_expression(recursive_rpn_expr);
    }

    return REDISMODULE_OK;
#undef SHIFT_ARGS

}

int SelvaHierarchy_AggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SelvaHierarchy_Aggregate(ctx, 0, argv, argc);
}

int SelvaHierarchy_AggregateRecursiveCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return SelvaHierarchy_Aggregate(ctx, 1, argv, argc);
}

static int Aggregate_OnLoad(RedisModuleCtx *ctx) {
    /*
     * Register commands.
     */
    if (RedisModule_CreateCommand(ctx, "selva.aggregate", SelvaHierarchy_AggregateCommand, "readonly", 2, 2, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "selva.aggregateRecursive", SelvaHierarchy_AggregateRecursiveCommand, "readonly", 2, 2, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
SELVA_ONLOAD(Aggregate_OnLoad);
