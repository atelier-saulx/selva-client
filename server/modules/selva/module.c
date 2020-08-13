#include <math.h>

#include "cdefs.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/test_util.h"

#include "svector.h"
#include "modify/modify.h"
#include "modify/async_task.h"
#include "modify/hierarchy.h"

#define FLAG_NO_ROOT    0x1
#define FLAG_NO_MERGE   0x2

#define FISSET_NO_ROOT(m) (((m) & FLAG_NO_ROOT) == FLAG_NO_ROOT)
#define FISSET_NO_MERGE(m) (((m) & FLAG_NO_MERGE) == FLAG_NO_MERGE)

int SelvaCommand_Flurpy(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    // init auto memory for created strings
    RedisModule_AutoMemory(ctx);

    RedisModuleString *keyStr =
            RedisModule_CreateString(ctx, "flurpypants", strlen("flurpypants"));
    RedisModuleString *val = RedisModule_CreateString(ctx, "hallo", strlen("hallo"));
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyStr, REDISMODULE_WRITE);
    for (int i = 0; i < 10000; i++) {
        RedisModule_StringSet(key, val);
        // RedisModuleCallReply *r = RedisModule_Call(ctx, "publish", "x", "y");
    }

    RedisModule_CloseKey(key);
    RedisModuleString *reply = RedisModule_CreateString(ctx, "hallo", strlen("hallo"));
    RedisModule_ReplyWithString(ctx, reply);
    return REDISMODULE_OK;
}

static void RedisModuleString2Selva_NodeId(Selva_NodeId nodeId, RedisModuleString *id) {
    TO_STR(id);

    id_str = RedisModule_StringPtrLen(id, &id_len);
    memset(nodeId, '\0', SELVA_NODE_ID_SIZE);
    memcpy(nodeId, id_str, min(id_len, SELVA_NODE_ID_SIZE));
}

static const char *sztok(const char *s, size_t size, size_t * restrict i) {
    const char *r = NULL;

    if (*i < size - 1) {
        r = s + *i;
        *i = *i + strnlen(r, size) + 1;
    }

    return r;
}

static int parse_flags(RedisModuleString *arg) {
    TO_STR(arg);
    int flags = 0;

    for (size_t i = 0; i < arg_len; i++) {
        flags |= arg_str[i] == 'N' ? FLAG_NO_ROOT : 0;
        flags |= arg_str[i] == 'M' ? FLAG_NO_MERGE : 0;
    }

    return flags;
}

/**
 * Parse $alias query from the command args if one exists.
 * @param out a vector for the query.
 *            The SVector must be initialized before calling this function.
 */
static void parse_alias_query(RedisModuleString **argv, int argc, SVector *out) {
    for (int i = 0; i < argc; i += 3) {
        RedisModuleString *type = argv[i];
        RedisModuleString *field = argv[i + 1];
        RedisModuleString *value = argv[i + 2];

        TO_STR(type, field, value);
        char type_code = type_str[0];

        if (type_code == SELVA_MODIFY_ARG_STRING_ARRAY && !strcmp(field_str, "$alias")) {
            const char *s;
            size_t j = 0;
            while ((s = sztok(value_str, value_len, &j))) {
                SVector_Insert(out, (void *)s);
            }
        }
    }
}

static RedisModuleKey *open_node(RedisModuleCtx *ctx, SelvaModify_Hierarchy *hierarchy, RedisModuleString *id, int no_root) {
    Selva_NodeId nodeId;

    RedisModuleString2Selva_NodeId(nodeId, id);

    /*
     * If this is a new node we need to create a hierarchy node for it.
     */
    if (!SelvaModify_HierarchyNodeExists(hierarchy, nodeId)) {
        size_t nr_parents = unlikely(no_root) ? 0 : 1;

        int err = SelvaModify_SetHierarchy(ctx, hierarchy, nodeId, nr_parents, ((Selva_NodeId []){ ROOT_NODE_ID }), 0, NULL);
        if (err) {
            RedisModule_ReplyWithError(ctx, hierarchyStrError[-err]);
            return NULL;
        }
    }

    return RedisModule_OpenKey(ctx, id, REDISMODULE_WRITE);
}

// TODO: clean this up
// id, R|N type, key, value [, ... type, key, value]]
int SelvaCommand_Modify(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    SelvaModify_Hierarchy *hierarchy;
    RedisModuleString *id = NULL;
    RedisModuleKey *id_key = NULL;
    svector_autofree SVector alias_query;
    int err = REDISMODULE_OK;

    SVector_Init(&alias_query, 5, NULL);

    if (argc < 3 || (argc - 3) % 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *hkey_name = RedisModule_CreateString(ctx, HIERARCHY_DEFAULT_KEY, sizeof(HIERARCHY_DEFAULT_KEY) - 1);
    hierarchy = SelvaModify_OpenHierarchyKey(ctx, hkey_name);
    if (!hierarchy) {
        RedisModule_ReplyWithError(ctx, hierarchyStrError[-SELVA_MODIFY_HIERARCHY_ENOENT]);
        return REDISMODULE_ERR;
    }

    /*
     * We use the ID generated by the client as the nodeId by default but later
     * on if an $alias entry is found then the following value will be discarded.
     */
    id = argv[1];

    /*
     * Look for $alias that would replace id.
     */
    parse_alias_query(argv + 3, argc - 3, &alias_query);
    if (SVector_Size(&alias_query) > 0) {
        RedisModuleKey *alias_key = open_aliases_key(ctx);

        /*
         * Replace id with the first match from alias_query.
         */
        char **it;
        SVECTOR_FOREACH(it, &alias_query) {
            RedisModuleString *tmp_id;

            if (!RedisModule_HashGet(alias_key, REDISMODULE_HASH_CFIELDS, *it, &tmp_id, NULL)) {
                Selva_NodeId nodeId;

                RedisModuleString2Selva_NodeId(nodeId, tmp_id);

                if (SelvaModify_HierarchyNodeExists(hierarchy, nodeId)) {
                    id = tmp_id;

                    /*
                     * If no match was found all the aliases should be assigned.
                     * If a match was found the query vector can be cleared now
                     * to prevent any aliases from being created.
                     */
                    SVector_Clear(&alias_query);

                    break;
                }
            }
        }

        RedisModule_CloseKey(alias_key);
    }

    const unsigned flags = parse_flags(argv[2]);
    const int no_root = FISSET_NO_ROOT(flags);

    id_key = open_node(ctx, hierarchy, id, no_root);
    if (!id_key) {
        TO_STR(id);
        char err_msg[80];

        snprintf(err_msg, sizeof(err_msg), "ERR Failed to open the key for id: \"%s\"", id_str);
        RedisModule_ReplyWithError(ctx, err_msg);
        return REDISMODULE_ERR;
    }

    /*
     * Parse the rest of the arguments.
     */
    for (int i = 3; i < argc; i += 3) {
        bool publish = true;
        RedisModuleString *type = argv[i];
        RedisModuleString *field = argv[i + 1];
        RedisModuleString *value = argv[i + 2];

        TO_STR(type, field, value);
        char type_code = type_str[0];

        size_t current_value_len = 0;
        RedisModuleString *current_value = NULL;
        const char *current_value_str = NULL;

        if (!RedisModule_HashGet(id_key, REDISMODULE_HASH_NONE, field, &current_value, NULL)) {
            current_value_str = RedisModule_StringPtrLen(current_value, &current_value_len);
        }

        if (type_code != SELVA_MODIFY_ARG_OP_INCREMENT && type_code != SELVA_MODIFY_ARG_OP_SET &&
                current_value && current_value_len == value_len &&
                !memcmp(current_value, value, min(current_value_len, value_len))) {
            // printf("Current value is equal to the specified value for key %s and value %s\n", field_str,
            //              value_str);
            continue;
        }

        if (type_code == SELVA_MODIFY_ARG_OP_INCREMENT) {
            struct SelvaModify_OpIncrement *incrementOpts = (struct SelvaModify_OpIncrement *)value_str;
            SelvaModify_ModifyIncrement(ctx, id_key, id, field, field_str, field_len,
                    current_value, current_value_str, current_value_len, incrementOpts);
        } else if (type_code == SELVA_MODIFY_ARG_OP_SET) {
            struct SelvaModify_OpSet *setOpts;

            setOpts = SelvaModify_OpSet_align(value);
            if (!setOpts) {
                char err_msg[80];

                snprintf(err_msg, sizeof(err_msg), "ERR Invalid argument at: %d", i);
                RedisModule_ReplyWithError(ctx, err_msg);
                goto out;
            }

            err = SelvaModify_ModifySet(ctx, hierarchy, id_key, id, field, setOpts);
            if (err) {
                goto out;
            }
        } else if (type_code == SELVA_MODIFY_ARG_STRING_ARRAY) {
            /*
             * $merge:
             */
            if (FISSET_NO_MERGE(flags) && !strcmp(field_str, "$merge")) {
                /* TODO Implement $merge */
            }
            /*
             * $alias: NOP
             */
        } else if (type_code == SELVA_MODIFY_ARG_OP_DEL) {
            err = SelvaModify_ModifyDel(ctx, hierarchy, id_key, id, field, value_str);
            if (err) {
                TO_STR(field);
                char err_msg[80];

                snprintf(err_msg, sizeof(err_msg), "ERR Failed to delete the field: \"%s\"", field_str);
                RedisModule_ReplyWithError(ctx, err_msg);
                goto out;
            }
        } else {
            if (type_code == SELVA_MODIFY_ARG_DEFAULT) {
                if (current_value != NULL) {
                    publish = false;
                } else {
                    RedisModule_HashSet(id_key, REDISMODULE_HASH_NONE, field, value, NULL);
                }
            } else if (type_code == SELVA_MODIFY_ARG_VALUE) {
                RedisModule_HashSet(id_key, REDISMODULE_HASH_NONE, field, value, NULL);
            } else {
                char err_msg[80];

                snprintf(err_msg, sizeof(err_msg), "ERR Invalid type: \"%c\"", type_code);
                RedisModule_ReplyWithError(ctx, err_msg);
                goto out;
            }
        }

        if (publish) {
            size_t id_len;
            const char *id_str = RedisModule_StringPtrLen(id, &id_len);

            SelvaModify_Publish(id_str, id_len, field_str, field_len);
        }
    }

    /*
     * If the size of alias_query is greater than zero it means that no match
     * was not found for $alias and we need to create all the aliases listed in
     * the query.
     * We know that the aliases are in an array so it's enough to get the
     * address of the first alias to have access to the whole array.
     */
    if (SVector_Size(&alias_query) > 0) {
        RedisModuleString *field = RedisModule_CreateString(ctx, "aliases", 7);
        struct SelvaModify_OpSet opSet = {
            .is_reference = 0,
            .$add = SVector_Peek(&alias_query),
            .$add_len = SVector_Size(&alias_query),
            .$delete = NULL,
            .$delete_len = 0,
            .$value = NULL,
            .$value_len = 0,
        };

        (void)SelvaModify_ModifySet(ctx, hierarchy, id_key, id, field, &opSet);
    }

    RedisModule_ReplyWithString(ctx, id);
    RedisModule_ReplicateVerbatim(ctx);
out:
    RedisModule_CloseKey(id_key);

    return err;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    int Hierarchy_OnLoad(RedisModuleCtx *ctx);

    // Register the module itself
    if (RedisModule_Init(ctx, "selva", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "selva.modify", SelvaCommand_Modify, "readonly", 1, 1, 1) ==
            REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "selva.flurpypants", SelvaCommand_Flurpy, "readonly", 1, 1,
                                                                1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return Hierarchy_OnLoad(ctx);
}
