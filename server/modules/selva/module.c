#define _GNU_SOURCE

#include <math.h>
#include <stddef.h>
#include <unistd.h>
#include "cdefs.h"
#include "config.h"
#include "redismodule.h"
#include "cstrings.h"
#include "libdeflate.h"
#include "typestr.h"
#include "selva.h"
#include "selva_onload.h"
#include "svector.h"
#include "bitmap.h"
#include "errors.h"
#include "async_task.h"
#include "hierarchy.h"
#include "timestamp.h"
#include "modify.h"
#include "rms.h"
#include "selva_object.h"
#include "selva_trace.h"
#include "subscriptions.h"

#define FLAG_NO_ROOT    0x01 /*!< Don't set root as a parent. */
#define FLAG_NO_MERGE   0x02 /*!< Clear any existing fields. */
#define FLAG_CREATE     0x04 /*!< Only create a new node or fail. */
#define FLAG_UPDATE     0x08 /*!< Only update an existing node. */

#define FISSET_NO_ROOT(m) (((m) & FLAG_NO_ROOT) == FLAG_NO_ROOT)
#define FISSET_NO_MERGE(m) (((m) & FLAG_NO_MERGE) == FLAG_NO_MERGE)
#define FISSET_CREATE(m) (((m) & FLAG_CREATE) == FLAG_CREATE)
#define FISSET_UPDATE(m) (((m) & FLAG_UPDATE) == FLAG_UPDATE)

#define REPLY_WITH_ARG_TYPE_ERROR(v) \
    replyWithSelvaErrorf(ctx, SELVA_EINTYPE, "Expected: %s", typeof_str(v))

/**
 * Modify op arg handler status.
 */
enum selva_op_repl_state {
    SELVA_OP_REPL_STATE_UNCHANGED,  /*!< No changes, do not replicate, reply with OK or ERR. */
    SELVA_OP_REPL_STATE_UPDATED,    /*!< Value changed, replicate, reply with UPDATED */
    SELVA_OP_REPL_STATE_REPLICATE,  /*!< Value might have changed, replicate, reply with OK */
};

/**
 * Struct type for replicating the automatic timestamps.
 */
struct replicate_ts {
    int8_t created;
    int8_t updated;
    long long created_at;
    long long updated_at;
};

SET_DECLARE(selva_onload, Selva_Onload);
SET_DECLARE(selva_onunld, Selva_Onunload);

SELVA_TRACE_HANDLE(cmd_modify);

/**
 * Get the replicate_ts struct.
 */
void get_replicate_ts(struct replicate_ts *rs, struct SelvaHierarchyNode *node, bool created, bool updated) {
    struct SelvaObject *obj = SelvaHierarchy_GetNodeObject(node);
    rs->created = created ? 1 : 0;
    rs->updated = updated ? 1 : 0;

    if (created) {
        (void)SelvaObject_GetLongLongStr(obj, SELVA_CREATED_AT_FIELD, sizeof(SELVA_CREATED_AT_FIELD) - 1, &rs->created_at);
    }
    if (updated) {
        (void)SelvaObject_GetLongLongStr(obj, SELVA_UPDATED_AT_FIELD, sizeof(SELVA_UPDATED_AT_FIELD) - 1, &rs->updated_at);
    }
}

/*
 * Replicate the selva.modify command.
 * This function depends on the argument order of selva.modify.
 */
void replicateModify(RedisModuleCtx *ctx, const struct bitmap *replset, RedisModuleString **orig_argv, const struct replicate_ts *rs) {
    const int leading_args = 3; /* [cmd_name, key, flags] */
    const long long count = bitmap_popcount(replset);
    RedisModuleString **argv;

    if (count == 0 && !rs->created && !rs->updated) {
        return; /* Skip. */
    }

    argv = RedisModule_PoolAlloc(ctx, ((size_t)((long long)leading_args + 3 * count + (rs->created + rs->updated) * 3)) * sizeof(RedisModuleString *));
    if (!argv) {
        fprintf(stderr, "%s:%d: Replication error: %s\n",
                __FILE__, __LINE__,
                getSelvaErrorStr(SELVA_ENOMEM));
        return;
    }

    /*
     * Copy the leading args.
     */
    int argc = leading_args;
    for (int i = 0; i < argc; i++) {
        argv[i] = orig_argv[i];
    }

    int i_arg_type = leading_args;
    for (int i = 0; i < (int)replset->nbits; i++) {
        if (bitmap_get(replset, i)) {
            argv[argc++] = orig_argv[i_arg_type];
            argv[argc++] = orig_argv[i_arg_type + 1];
            argv[argc++] = orig_argv[i_arg_type + 2];
        }
        i_arg_type += 3;
    }

    if (rs->created) {
        const char op[2] = { SELVA_MODIFY_ARG_LONGLONG, '\0' };
        const size_t size = sizeof(rs->created_at);
        char v[size];

        memcpy(v, &rs->created_at, size);
        argv[argc++] = RedisModule_CreateString(ctx, op, 1);
        argv[argc++] = RedisModule_CreateString(ctx, SELVA_CREATED_AT_FIELD, sizeof(SELVA_CREATED_AT_FIELD) - 1);
        argv[argc++] = RedisModule_CreateString(ctx, v, size);
    }
    if (rs->updated) {
        const char op[2] = { SELVA_MODIFY_ARG_LONGLONG, '\0' };
        const size_t size = sizeof(rs->updated_at);
        char v[size];

        memcpy(v, &rs->updated_at, size);
        argv[argc++] = RedisModule_CreateString(ctx, op, 1);
        argv[argc++] = RedisModule_CreateString(ctx, SELVA_UPDATED_AT_FIELD, sizeof(SELVA_UPDATED_AT_FIELD) - 1);
        argv[argc++] = RedisModule_CreateString(ctx, v, size);
    }

#if DEBUG_MODIFY_REPLICATION_DELAY_NS > 0
    usleep(DEBUG_MODIFY_REPLICATION_DELAY_NS);
#endif

    RedisModule_ReplicateVerbatimArgs(ctx, argv, argc);
}

/*
 * Tokenize nul-terminated strings from a string with the size of size.
 */
static const char *sztok(const char *s, size_t size, size_t * restrict i) {
    const char *r = NULL;

    if (size == 0) {
        return NULL;
    }

    if (*i < size - 1) {
        r = s + *i;
        *i = *i + strnlen(r, size) + 1;
    }

    return r;
}

static int parse_flags(const RedisModuleString *arg) {
    TO_STR(arg);
    int flags = 0;

    for (size_t i = 0; i < arg_len; i++) {
        flags |= arg_str[i] == 'N' ? FLAG_NO_ROOT : 0;
        flags |= arg_str[i] == 'M' ? FLAG_NO_MERGE : 0;
        flags |= arg_str[i] == 'C' ? FLAG_CREATE : 0;
        flags |= arg_str[i] == 'U' ? FLAG_UPDATE : 0;
    }

    return flags;
}

static int in_mem_range(const void *p, const void *start, size_t size) {
    return (ptrdiff_t)p >= (ptrdiff_t)start && (ptrdiff_t)p < (ptrdiff_t)start + (ptrdiff_t)size;
}

static bool wasImplicitlyCreated(struct SelvaHierarchyNode *node) {
    struct SelvaObject *obj;
    enum SelvaObjectType type;

    obj = SelvaHierarchy_GetNodeObject(node);
    type = SelvaObject_GetTypeStr(obj, SELVA_TYPE_FIELD, sizeof(SELVA_TYPE_FIELD) - 1);

    /*
     * Nodes created implicitly won't have a type yet.
     */
    return type == SELVA_OBJECT_NULL;
}

static struct SelvaModify_OpSet *SelvaModify_OpSet_align(RedisModuleCtx *ctx, const struct RedisModuleString *data) {
    TO_STR(data);
    struct SelvaModify_OpSet *op;

    /* TODO Support __ORDER_BIG_ENDIAN__ */
    _Static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "Only little endian host is supported");

    if (!data_str && data_len < sizeof(struct SelvaModify_OpSet)) {
        return NULL;
    }

    op = RedisModule_PoolAlloc(ctx, data_len);
    if (!op) {
        return NULL;
    }

    memcpy(op, data_str, data_len);
    op->$add    = op->$add    ? ((char *)op + (ptrdiff_t)op->$add)    : NULL;
    op->$delete = op->$delete ? ((char *)op + (ptrdiff_t)op->$delete) : NULL;
    op->$value  = op->$value  ? ((char *)op + (ptrdiff_t)op->$value)  : NULL;

    if (!(((!op->$add    && op->$add_len == 0)    || (in_mem_range(op->$add,    op, data_len) && in_mem_range(op->$add    + op->$add_len    - 1,  op, data_len))) &&
          ((!op->$delete && op->$delete_len == 0) || (in_mem_range(op->$delete, op, data_len) && in_mem_range(op->$delete + op->$delete_len - 1,  op, data_len))) &&
          ((!op->$value  && op->$value_len == 0)  || (in_mem_range(op->$value,  op, data_len) && in_mem_range(op->$value  + op->$value_len  - 1,  op, data_len)))
       )) {
        return NULL;
    }

    return op;
}

static struct SelvaModify_OpEdgeMeta *SelvaModify_OpEdgeMeta_align(RedisModuleCtx *ctx, const struct RedisModuleString *data) {
    TO_STR(data);
    struct SelvaModify_OpEdgeMeta *op;

    /* TODO Support __ORDER_BIG_ENDIAN__ */
    _Static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "Only little endian host is supported");

    if (!data_str && data_len < sizeof(struct SelvaModify_OpEdgeMeta)) {
        return NULL;
    }

    op = RedisModule_PoolAlloc(ctx, data_len);
    if (!op) {
        return NULL;
    }

    memcpy(op, data_str, data_len);
    if (!op->meta_field_name_str || !op->meta_field_value_str) {
        return NULL;
    }

    op->meta_field_name_str = ((char *)op + (ptrdiff_t)op->meta_field_name_str);
    op->meta_field_value_str = ((char *)op + (ptrdiff_t)op->meta_field_value_str);

    if (!((in_mem_range(op->meta_field_name_str,  op, data_len) && in_mem_range(op->meta_field_name_str  + op->meta_field_name_len  - 1, op, data_len)) &&
          (in_mem_range(op->meta_field_value_str, op, data_len) && in_mem_range(op->meta_field_value_str + op->meta_field_value_len - 1, op, data_len)))) {
        return NULL;
    }

    return op;
}

/**
 * Parse $alias query from the command args if one exists.
 * @param out a vector for the query.
 *            The SVector must be initialized before calling this function.
 */
static void parse_alias_query(RedisModuleString **argv, int argc, SVector *out) {
    for (int i = 0; i < argc; i += 3) {
        const RedisModuleString *type = argv[i];
        const RedisModuleString *field = argv[i + 1];
        const RedisModuleString *value = argv[i + 2];

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

enum selva_op_repl_state handle_modify_arg_op_obj_meta(
        RedisModuleCtx *ctx,
        struct SelvaObject *obj,
        const RedisModuleString *field,
        const RedisModuleString *value) {
    TO_STR(value);
    SelvaObjectMeta_t new_user_meta;
    SelvaObjectMeta_t old_user_meta;
    int err;

    if (value_len < sizeof(SelvaObjectMeta_t)) {
        REPLY_WITH_ARG_TYPE_ERROR(new_user_meta);
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    memcpy(&new_user_meta, value_str, sizeof(SelvaObjectMeta_t));
    err = SelvaObject_SetUserMeta(obj, field, new_user_meta, &old_user_meta);
    if (err) {
        replyWithSelvaErrorf(ctx, err, "Failed to set key metadata (%s)",
                RedisModule_StringPtrLen(field, NULL));
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    if (new_user_meta != old_user_meta) {
        return SELVA_OP_REPL_STATE_UPDATED;
    }

    return SELVA_OP_REPL_STATE_REPLICATE;
}

static enum selva_op_repl_state modify_array_op(
        RedisModuleCtx *ctx,
        struct SelvaHierarchyNode *node,
        int *active_insert_idx,
        int has_push,
        char type_code,
        RedisModuleString *field,
        RedisModuleString *value) {
    TO_STR(field, value);
    ssize_t idx;

    const int ary_err = get_array_field_index(field_str, field_len, &idx);
    if (ary_err != 0) {
        replyWithSelvaErrorf(ctx, SELVA_EINVAL, "Invalid array index");
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    struct SelvaObject *obj = SelvaHierarchy_GetNodeObject(node);
    const ssize_t new_len = (const char *)memrchr(field_str, '[', field_len) - field_str;

    if (idx == -1) {
        const int ary_len = (int)SelvaObject_GetArrayLenStr(obj, field_str, new_len);

        idx = ary_len - 1 + has_push;
        if (idx < 0) {
            /* TODO No idea what should be the error but the previous error was bogus. */
            replyWithSelvaErrorf(ctx, SELVA_EINTYPE, "Unable to set value to array index %d", idx);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    }

    if (type_code == SELVA_MODIFY_ARG_STRING) {
        int err;

        if (*active_insert_idx == idx) {
            err = SelvaObject_InsertArrayIndexStr(obj, field_str, new_len, SELVA_OBJECT_STRING, idx, value);
            *active_insert_idx = -1;
        } else {
            err = SelvaObject_AssignArrayIndexStr(obj, field_str, new_len, SELVA_OBJECT_STRING, idx, value);
        }

        if (err) {
            replyWithSelvaErrorf(ctx, err, "Failed to set a string value");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        RedisModule_RetainString(ctx, value);
    } else if (type_code == SELVA_MODIFY_ARG_DOUBLE) {
        int err;
        union {
            char s[sizeof(double)];
            double d;
            void *p;
        } v = {
            .d = 0.0,
        };

        if (value_len != sizeof(v.d)) {
            REPLY_WITH_ARG_TYPE_ERROR(v.d);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        memcpy(v.s, value_str, sizeof(v.d));

        if (*active_insert_idx == idx) {
            err = SelvaObject_InsertArrayIndexStr(obj, field_str, new_len, SELVA_OBJECT_DOUBLE, idx, v.p);
            *active_insert_idx = -1;
        } else {
            err = SelvaObject_AssignArrayIndexStr(obj, field_str, new_len, SELVA_OBJECT_DOUBLE, idx, v.p);
        }

        if (err) {
            replyWithSelvaErrorf(ctx, err, "Failed to set a double value");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (type_code == SELVA_MODIFY_ARG_LONGLONG) {
        int err;
        union {
            char s[sizeof(double)];
            long long ll;
            void *p;
        } v = {
            .ll = 0,
        };

        if (value_len != sizeof(v.ll)) {
            REPLY_WITH_ARG_TYPE_ERROR(v.ll);
            return 0;
        }

        memcpy(v.s, value_str, sizeof(v.ll));

        if (*active_insert_idx == idx) {
            err = SelvaObject_InsertArrayIndexStr(obj, field_str, new_len, SELVA_OBJECT_LONGLONG, idx, v.p);
            *active_insert_idx = -1;
        } else {
            err = SelvaObject_AssignArrayIndexStr(obj, field_str, new_len, SELVA_OBJECT_LONGLONG, idx, v.p);
        }

        if (err) {
            replyWithSelvaErrorf(ctx, err, "Failed to set a long value");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (type_code == SELVA_MODIFY_ARG_OP_OBJ_META) {
        return handle_modify_arg_op_obj_meta(ctx, obj, field, value);
    } else {
        replyWithSelvaErrorf(ctx, SELVA_EINTYPE, "ERR Invalid operation type with array syntax: \"%c\"", type_code);
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    return SELVA_OP_REPL_STATE_UPDATED;
}

static enum selva_op_repl_state modify_op(
        RedisModuleCtx *ctx,
        SelvaHierarchy *hierarchy,
        const Selva_NodeId nodeId,
        struct SelvaHierarchyNode *node,
        char type_code,
        RedisModuleString *field,
        RedisModuleString *value) {
    struct SelvaObject *obj = SelvaHierarchy_GetNodeObject(node);
    TO_STR(field, value);

    if (type_code == SELVA_MODIFY_ARG_OP_INCREMENT) {
        const struct SelvaModify_OpIncrement *incrementOpts = (const struct SelvaModify_OpIncrement *)value_str;
        int err;

        err = SelvaObject_IncrementLongLong(obj, field, incrementOpts->$default, incrementOpts->$increment);
        if (err) {
            replyWithSelvaError(ctx, err);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (type_code == SELVA_MODIFY_ARG_OP_INCREMENT_DOUBLE) {
        const struct SelvaModify_OpIncrementDouble *incrementOpts = (const struct SelvaModify_OpIncrementDouble*)value_str;
        int err;

        err = SelvaObject_IncrementDouble(obj, field, incrementOpts->$default, incrementOpts->$increment);
        if (err) {
            replyWithSelvaError(ctx, err);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (type_code == SELVA_MODIFY_ARG_OP_SET) {
        struct SelvaModify_OpSet *setOpts;
        int err;

        setOpts = SelvaModify_OpSet_align(ctx, value);
        if (!setOpts) {
            replyWithSelvaErrorf(ctx, SELVA_EINVAL, "Invalid OpSet");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        err = SelvaModify_ModifySet(ctx, hierarchy, nodeId, node, obj, field, setOpts);
        if (err == 0) {
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        } else if (err < 0) {
            replyWithSelvaError(ctx, err);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (type_code == SELVA_MODIFY_ARG_OP_DEL) {
        int err;

        err = SelvaModify_ModifyDel(ctx, hierarchy, node, obj, field);
        if (err == SELVA_ENOENT) {
            /* No need to replicate. */
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        } else if (err) {
            replyWithSelvaErrorf(ctx, err, "Failed to delete the field: \"%.*s\"", (int)field_len, field_str);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (type_code == SELVA_MODIFY_ARG_DEFAULT_STRING ||
            type_code == SELVA_MODIFY_ARG_STRING) {
        const enum SelvaObjectType old_type = SelvaObject_GetTypeStr(obj, field_str, field_len);
        RedisModuleString *old_value;
        RedisModuleString *shared;
        int err;

        if (type_code == SELVA_MODIFY_ARG_DEFAULT_STRING && old_type != SELVA_OBJECT_NULL) {
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        if (old_type == SELVA_OBJECT_STRING && !SelvaObject_GetString(obj, field, &old_value)) {
            TO_STR(old_value);

            if (old_value_len == value_len && !memcmp(old_value_str, value_str, value_len)) {
                RedisModule_ReplyWithSimpleString(ctx, "OK");
                return SELVA_OP_REPL_STATE_UNCHANGED;
            }
        }

        shared = Share_RMS(field_str, field_len, value);
        if (shared) {
            err = SelvaObject_SetString(obj, field, shared);
            if (err) {
                RedisModule_FreeString(NULL, shared);
                replyWithSelvaErrorf(ctx, err, "Failed to set a shared string value");
                return SELVA_OP_REPL_STATE_UNCHANGED;
            }
        } else {
            err = SelvaObject_SetString(obj, field, value);
            if (err) {
                replyWithSelvaErrorf(ctx, err, "Failed to set a string value");
                return SELVA_OP_REPL_STATE_UNCHANGED;
            }
            RedisModule_RetainString(ctx, value);
        }
    } else if (type_code == SELVA_MODIFY_ARG_DEFAULT_LONGLONG ||
               type_code == SELVA_MODIFY_ARG_LONGLONG) {
        long long ll;
        int err;

        if (value_len != sizeof(ll)) {
            REPLY_WITH_ARG_TYPE_ERROR(ll);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        memcpy(&ll, value_str, sizeof(ll));

        if (type_code == SELVA_MODIFY_ARG_DEFAULT_LONGLONG) {
            err = SelvaObject_SetLongLongDefault(obj, field, ll);
        } else {
            long long old_value;

            if (!SelvaObject_GetLongLong(obj, field, &old_value)) {
                if (old_value == ll) {
                    RedisModule_ReplyWithSimpleString(ctx, "OK");
                    return SELVA_OP_REPL_STATE_UNCHANGED;
                }
            }

            err = SelvaObject_SetLongLong(obj, field, ll);
        }
        if (err == SELVA_EEXIST) { /* Default handling. */
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        } else if (err) {
            replyWithSelvaError(ctx, err);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (type_code == SELVA_MODIFY_ARG_DEFAULT_DOUBLE ||
               type_code == SELVA_MODIFY_ARG_DOUBLE) {
        double d;
        int err;

        if (value_len != sizeof(d)) {
            REPLY_WITH_ARG_TYPE_ERROR(d);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        memcpy(&d, value_str, sizeof(d));

        if (type_code == SELVA_MODIFY_ARG_DEFAULT_DOUBLE) {
            err = SelvaObject_SetDoubleDefault(obj, field, d);
        } else {
            double old_value;

            if (!SelvaObject_GetDouble(obj, field, &old_value)) {
                if (old_value == d) {
                    RedisModule_ReplyWithSimpleString(ctx, "OK");
                    return SELVA_OP_REPL_STATE_UNCHANGED;
                }
            }

            err = SelvaObject_SetDouble(obj, field, d);
        }
        if (err == SELVA_EEXIST) { /* Default handling. */
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        } else if (err) {
            replyWithSelvaError(ctx, err);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (type_code == SELVA_MODIFY_ARG_OP_OBJ_META) {
        return handle_modify_arg_op_obj_meta(ctx, obj, field, value);
    } else if (type_code == SELVA_MODIFY_ARG_OP_ARRAY_REMOVE) {
        uint32_t v;
        int err;

        if (value_len != sizeof(uint32_t)) {
            REPLY_WITH_ARG_TYPE_ERROR(v);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        memcpy(&v, value_str, sizeof(uint32_t));

        err = SelvaObject_RemoveArrayIndexStr(obj, field_str, field_len, v);
        if (err) {
            replyWithSelvaErrorf(ctx, err, "Failed to remove array index (%.*s.%s)",
                    (int)field_len, field_str);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else {
        replyWithSelvaErrorf(ctx, SELVA_EINTYPE, "ERR Invalid type: \"%c\"", type_code);
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    return SELVA_OP_REPL_STATE_UPDATED;
}

static enum selva_op_repl_state modify_edge_meta_op(
        RedisModuleCtx *ctx,
        struct SelvaHierarchyNode *node,
        RedisModuleString *field,
        RedisModuleString *raw_value) {
    TO_STR(field);
    struct EdgeField *edge_field;
    struct SelvaObject *edge_metadata;
    const struct SelvaModify_OpEdgeMeta *op;
    enum SelvaModify_OpEdgetMetaCode op_code;
    int err;

    edge_field = Edge_GetField(node, field_str, field_len);
    if (!edge_field) {
        replyWithSelvaErrorf(ctx, SELVA_ENOENT, "Edge field not found");
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    op = SelvaModify_OpEdgeMeta_align(ctx, raw_value);
    if (!op) {
        replyWithSelvaError(ctx, SELVA_EINVAL);
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    if (op->delete_all) {
        Edge_DeleteFieldMetadata(edge_field);
        return SELVA_OP_REPL_STATE_UPDATED;
    }

    err = Edge_GetFieldEdgeMetadata(edge_field, op->dst_node_id, 1, &edge_metadata);
    if (err) {
        replyWithSelvaErrorf(ctx, err, "Failed to get the metadata object");
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    op_code = op->op_code;
    if (op_code == SELVA_MODIFY_OP_EDGE_META_DEFAULT_STRING ||
        op_code == SELVA_MODIFY_OP_EDGE_META_STRING) {
        const enum SelvaObjectType old_type = SelvaObject_GetTypeStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len);
        RedisModuleString *old_value;
        RedisModuleString *meta_field_value;

        if (op_code == SELVA_MODIFY_OP_EDGE_META_DEFAULT_STRING && old_type != SELVA_OBJECT_NULL) {
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        if (old_type == SELVA_OBJECT_STRING && !SelvaObject_GetStringStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len, &old_value)) {
            TO_STR(old_value);

            if (old_value_len == op->meta_field_value_len && !memcmp(old_value_str, op->meta_field_value_str, op->meta_field_value_len)) {
                RedisModule_ReplyWithSimpleString(ctx, "OK");
                return SELVA_OP_REPL_STATE_UNCHANGED;
            }
        }

        meta_field_value = RedisModule_CreateString(NULL, op->meta_field_value_str, op->meta_field_value_len);
        if (!meta_field_value) {
            replyWithSelvaError(ctx, SELVA_ENOMEM);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        err = SelvaObject_SetStringStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len, meta_field_value);
        if (err) {
            RedisModule_FreeString(NULL, meta_field_value);
            replyWithSelvaErrorf(ctx, err, "Failed to set a string value");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (op_code == SELVA_MODIFY_OP_EDGE_META_DEFAULT_LONGLONG ||
               op_code == SELVA_MODIFY_OP_EDGE_META_LONGLONG) {
        long long ll;

        if (op->meta_field_value_len != sizeof(ll)) {
            REPLY_WITH_ARG_TYPE_ERROR(ll);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        memcpy(&ll, op->meta_field_value_str, sizeof(ll));

        if (op_code == SELVA_MODIFY_OP_EDGE_META_DEFAULT_LONGLONG) {
            err = SelvaObject_SetLongLongDefaultStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len, ll);
        } else {
            long long old_value;

            if (!SelvaObject_GetLongLongStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len, &old_value)) {
                if (old_value == ll) {
                    RedisModule_ReplyWithSimpleString(ctx, "OK");
                    return SELVA_OP_REPL_STATE_UNCHANGED;
                }
            }

            err = SelvaObject_SetLongLongStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len, ll);
        }
        if (err == SELVA_EEXIST) { /* Default handling */
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        } else if (err) {
            replyWithSelvaError(ctx, err);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (op_code == SELVA_MODIFY_OP_EDGE_META_DEFAULT_DOUBLE ||
               op_code == SELVA_MODIFY_OP_EDGE_META_DOUBLE) {
        double d;

        if (op->meta_field_value_len != sizeof(d)) {
            REPLY_WITH_ARG_TYPE_ERROR(d);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }

        memcpy(&d, op->meta_field_value_str, sizeof(d));

        if (op_code == SELVA_MODIFY_OP_EDGE_META_DEFAULT_DOUBLE) {
            err = SelvaObject_SetDoubleDefaultStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len, d);
        } else {
            double old_value;

            if (!SelvaObject_GetDoubleStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len, &old_value)) {
                if (old_value == d) {
                    RedisModule_ReplyWithSimpleString(ctx, "OK");
                    return SELVA_OP_REPL_STATE_UNCHANGED;
                }
            }

            err = SelvaObject_SetDoubleStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len, d);
        }
        if (err == SELVA_EEXIST) { /* Default handling. */
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        } else if (err) {
            replyWithSelvaError(ctx, err);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else if (op_code == SELVA_MODIFY_OP_EDGE_META_DEL) {
        err = SelvaObject_DelKeyStr(edge_metadata, op->meta_field_name_str, op->meta_field_name_len);
        if (err == SELVA_ENOENT) {
            /* No need to replicate. */
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            return SELVA_OP_REPL_STATE_UNCHANGED;
        } else if (err) {
            replyWithSelvaError(ctx, err);
            return SELVA_OP_REPL_STATE_UNCHANGED;
        }
    } else {
        replyWithSelvaError(ctx, SELVA_EINTYPE);
        return SELVA_OP_REPL_STATE_UNCHANGED;
    }

    return SELVA_OP_REPL_STATE_UPDATED;
}

/*
 * Request:
 * id, FLAGS type, field, value [, ... type, field, value]]
 * N = No root
 * M = Merge
 *
 * The behavior and meaning of `value` depends on `type` (enum SelvaModify_ArgType).
 *
 * Response:
 * [
 * id,
 * [err | 0 | 1]
 * ...
 * ]
 *
 * err = error in parsing or executing the triplet
 * OK = the triplet made no changes
 * UPDATED = changes made and replicated
 */
int SelvaCommand_Modify(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    SELVA_TRACE_BEGIN_AUTO(cmd_modify);
    RedisModule_AutoMemory(ctx);
    SelvaHierarchy *hierarchy;
    RedisModuleString *id = NULL;
    SVECTOR_AUTOFREE(alias_query);
    bool created = false; /* Will be set if the node was created during this command. */
    bool updated = false;
    bool new_alias = false; /* Set if $alias will be creating new alias(es). */
    int err = 0;

    /*
     * The comparator must be NULL to ensure that the vector is always stored
     * as an array as that is required later on for the modify op.
     */
    SVector_Init(&alias_query, 5, NULL);

    /*
     * We expect two fixed arguments and a number of [type, field, value] triplets.
     */
    if (argc < 3 || (argc - 3) % 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *hkey_name = RedisModule_CreateString(ctx, HIERARCHY_DEFAULT_KEY, sizeof(HIERARCHY_DEFAULT_KEY) - 1);
    hierarchy = SelvaModify_OpenHierarchy(ctx, hkey_name, REDISMODULE_READ | REDISMODULE_WRITE);
    if (!hierarchy) {
        return REDISMODULE_OK;
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

        if (alias_key && RedisModule_KeyType(alias_key) == REDISMODULE_KEYTYPE_HASH) {
            struct SVectorIterator it;
            char *str;

            /*
             * Replace id with the first match from alias_query.
             */
            SVector_ForeachBegin(&it, &alias_query);
            while ((str = SVector_Foreach(&it))) {
                RedisModuleString *tmp_id;

                if (!RedisModule_HashGet(alias_key, REDISMODULE_HASH_CFIELDS, str, &tmp_id, NULL)) {
                    Selva_NodeId nodeId;

                    err = Selva_RMString2NodeId(nodeId, tmp_id);
                    if (err) {
                        /* TODO Should it fail? */
                        continue;
                    }

                    if (SelvaHierarchy_NodeExists(hierarchy, nodeId)) {
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
        } else {
#if 0
            /* This is probably ok and it's a sign that there are no aliases in the DB yet. */
            fprintf(stderr, "%s:%d: Unable open aliases key or its type is invalid\n",
                    __FILE__, __LINE__);
#endif
            new_alias = true;
        }

        RedisModule_CloseKey(alias_key);
    }

    Selva_NodeId nodeId;
    struct SelvaHierarchyNode *node;
    const unsigned flags = parse_flags(argv[2]);

    err = Selva_RMString2NodeId(nodeId, id);
    if (err) {
        return replyWithSelvaErrorf(ctx, err, "Invalid nodeId");
    }

    node = SelvaHierarchy_FindNode(hierarchy, nodeId);
    if (!node) {
        if (FISSET_UPDATE(flags)) {
            /* if the specified id doesn't exist but $operation: 'update' specified */
            RedisModule_ReplyWithNull(ctx);
            return REDISMODULE_OK;
        }

        const size_t nr_parents = FISSET_NO_ROOT(flags) ? 0 : 1;

        err = SelvaModify_SetHierarchy(ctx, hierarchy, nodeId, nr_parents, ((Selva_NodeId []){ ROOT_NODE_ID }), 0, NULL, &node);
        if (err < 0) {
            return replyWithSelvaErrorf(ctx, err, "ERR Failed to initialize the node hierarchy for id: \"%s\"", RedisModule_StringPtrLen(id, NULL));
        }
        created = true;
    } else if (FISSET_CREATE(flags)) {
        /* if the specified id exists but $operation: 'insert' specified. */
        RedisModule_ReplyWithNull(ctx);
        return REDISMODULE_OK;
    }

    created = created || wasImplicitlyCreated(node);

    SelvaSubscriptions_FieldChangePrecheck(ctx, hierarchy, node);

    if (!created && FISSET_NO_MERGE(flags)) {
        SelvaHierarchy_ClearNodeFields(SelvaHierarchy_GetNodeObject(node));
    }

    /*
     * Replication bitmap.
     *
     * bit  desc
     * 0    replicate the first triplet
     * 1    replicate the second triplet
     * ...  ...
     */
    const int nr_triplets = (argc - 3) / 3;
    struct bitmap *replset = RedisModule_PoolAlloc(ctx, BITMAP_ALLOC_SIZE(nr_triplets));

    if (!replset) {
        return replyWithSelvaErrorf(ctx, SELVA_ENOMEM, "Failed to allocate memory for replication");
    }
    replset->nbits = nr_triplets;
    bitmap_erase(replset);

    int has_push = 0;
    int active_insert_idx = -1;

    /*
     * Parse the rest of the arguments and run the modify operations.
     * Each part of the command will send a separate response back to the client.
     * Each part is also replicated separately.
     */
    RedisModule_ReplyWithArray(ctx, 1 + nr_triplets);
    RedisModule_ReplyWithString(ctx, id);

    for (int i = 3; i < argc; i += 3) {
        RedisModuleString *type = argv[i];
        RedisModuleString *field = argv[i + 1];
        RedisModuleString *value = argv[i + 2];
        TO_STR(type, field);
        const char type_code = type_str[0]; /* [0] always points to a valid char in RM_String. */
        enum selva_op_repl_state repl_state = SELVA_OP_REPL_STATE_UNCHANGED;

        if (get_array_field_index(field_str, field_len, NULL) == 0) {
            repl_state = modify_array_op(ctx, node, &active_insert_idx, has_push, type_code, field, value);
        } else if (type_code == SELVA_MODIFY_ARG_OP_ARRAY_PUSH) {
            TO_STR(value);
            uint32_t item_type;

            if (value_len != sizeof(uint32_t)) {
                replyWithSelvaErrorf(ctx, SELVA_EINVAL, "Invalid item type");
                continue;
            }

            memcpy(&item_type, value_str, sizeof(uint32_t));
            if (item_type == SELVA_OBJECT_OBJECT) {
                struct SelvaObject *obj = SelvaHierarchy_GetNodeObject(node);
                struct SelvaObject *new_obj;
                int err;

                new_obj = SelvaObject_New();
                if (!new_obj) {
                    replyWithSelvaErrorf(ctx, SELVA_ENOMEM, "Failed to push new object to array index (%.*s.%s)",
                            (int)field_len, field_str);
                    continue;
                }

                err = SelvaObject_InsertArrayStr(obj, field_str, field_len, SELVA_OBJECT_OBJECT, new_obj);
                if (err) {
                    replyWithSelvaErrorf(ctx, err, "Failed to push new object to array index (%.*s.%s)",
                            (int)field_len, field_str);
                    continue;
                }
            } else {
                has_push = 1;
            }

            repl_state = SELVA_OP_REPL_STATE_UPDATED;
        } else if (type_code == SELVA_MODIFY_ARG_OP_ARRAY_INSERT) {
            TO_STR(value);
            uint32_t item_type;
            uint32_t insert_idx;

            if (value_len != 2 * sizeof(uint32_t)) {
                replyWithSelvaErrorf(ctx, SELVA_EINTYPE, "Expected: int[2]");
                continue;
            }

            memcpy(&item_type, value_str, sizeof(uint32_t));
            memcpy(&insert_idx, value_str + sizeof(uint32_t), sizeof(uint32_t));

            if (item_type == SELVA_OBJECT_OBJECT) {
                struct SelvaObject *obj = SelvaHierarchy_GetNodeObject(node);
                struct SelvaObject *new_obj = SelvaObject_New();
                int err;

                if (!new_obj) {
                    replyWithSelvaErrorf(ctx, SELVA_ENOMEM, "Failed to push new object to array index (%.*s.%s)",
                            (int)field_len, field_str);
                    continue;
                }

                err = SelvaObject_InsertArrayIndexStr(obj, field_str, field_len, SELVA_OBJECT_OBJECT, insert_idx, new_obj);
                if (err) {
                    SelvaObject_Destroy(new_obj);

                    replyWithSelvaErrorf(ctx, err, "Failed to push new object to array index (%.*s.%s)",
                            (int)field_len, field_str);
                    continue;
                }
            } else {
                active_insert_idx = insert_idx;
            }

            repl_state = SELVA_OP_REPL_STATE_UPDATED;
        } else if (type_code == SELVA_MODIFY_ARG_STRING_ARRAY) {
            /*
             * Currently the $alias query is the only operation using string arrays.
             * $alias: NOP
             */
            if (new_alias) {
                RedisModule_ReplyWithSimpleString(ctx, "UPDATED");
            } else {
                RedisModule_ReplyWithSimpleString(ctx, "OK");
            }

            /* This triplet needs to be replicated. */
            bitmap_set(replset, i / 3 - 1);
            continue;
        } else if (type_code == SELVA_MODIFY_ARG_OP_EDGE_META) {
            repl_state = modify_edge_meta_op(ctx, node, field, value);
        } else {
            repl_state = modify_op(ctx, hierarchy, nodeId, node, type_code, field, value);
        }

        if (repl_state == SELVA_OP_REPL_STATE_REPLICATE) {
            /* This triplet needs to be replicated. */
            bitmap_set(replset, i / 3 - 1);

            RedisModule_ReplyWithSimpleString(ctx, "OK");
        } else if (repl_state == SELVA_OP_REPL_STATE_UPDATED) {
            /* This triplet needs to be replicated. */
            bitmap_set(replset, i / 3 - 1);

            /*
             * Publish that the field was changed.
             * Hierarchy handles events for parents and children.
             */
            if (strcmp(field_str, SELVA_PARENTS_FIELD) && strcmp(field_str, SELVA_CHILDREN_FIELD)) {
                SelvaSubscriptions_DeferFieldChangeEvents(ctx, hierarchy, node, field_str, field_len);
            }

            RedisModule_ReplyWithSimpleString(ctx, "UPDATED");
            updated = true;
        }
    }

    /*
     * If the size of alias_query is greater than zero it means that no match
     * was found for $alias and we need to create all the aliases listed in the
     * query.
     */
    if (SVector_Size(&alias_query) > 0) {
        struct SelvaObject *obj = SelvaHierarchy_GetNodeObject(node);
        RedisModuleString *aliases_field = RedisModule_CreateString(ctx, SELVA_ALIASES_FIELD, sizeof(SELVA_ALIASES_FIELD) - 1);
        struct SVectorIterator it;
        char *alias;

        SVector_ForeachBegin(&it, &alias_query);
        while ((alias = SVector_Foreach(&it))) {
            struct SelvaModify_OpSet opSet = {
                .op_set_type = SELVA_MODIFY_OP_SET_TYPE_CHAR,
                .$add = alias,
                .$add_len = strlen(alias) + 1, /* This is safe because the ultimate source is a RedisModuleString. */
                .$delete = NULL,
                .$delete_len = 0,
                .$value = NULL,
                .$value_len = 0,
            };

            err = SelvaModify_ModifySet(ctx, hierarchy, nodeId, node, obj, aliases_field, &opSet);
            if (err < 0) {
                TO_STR(id);

                /*
                 * Since we are already at the end of the command, it's next to
                 * impossible to rollback the command, so we'll just log any
                 * errors received here.
                 */
                fprintf(stderr, "%s:%d: An error occurred while setting an alias \"%s\" -> %s: %s\n",
                        __FILE__, __LINE__, alias, id_str, getSelvaErrorStr(err));
            }
        }
    }

    if (created) {
        SelvaSubscriptions_DeferTriggerEvents(ctx, hierarchy, node, SELVA_SUBSCRIPTION_TRIGGER_TYPE_CREATED);
    }
    if (updated && !created) {
        /*
         * If nodeId wasn't created by this command call but it was updated
         * then we need to defer the updated trigger.
         */
        SelvaSubscriptions_DeferTriggerEvents(ctx, hierarchy, node, SELVA_SUBSCRIPTION_TRIGGER_TYPE_UPDATED);

        if (!(RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_REPLICATED)) {
            struct SelvaObject *obj = SelvaHierarchy_GetNodeObject(node);
            const long long now = ts_now();

            /*
             * If the node was created then the field was already updated by hierarchy.
             * If the command was replicated then the master should send us the correct
             * timestamp.
             */
            SelvaObject_SetLongLongStr(obj, SELVA_UPDATED_AT_FIELD, sizeof(SELVA_UPDATED_AT_FIELD) - 1, now);
            SelvaSubscriptions_DeferFieldChangeEvents(ctx, hierarchy, node, SELVA_UPDATED_AT_FIELD, sizeof(SELVA_UPDATED_AT_FIELD) - 1);
        }
    }

    if (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_MASTER) {
        struct replicate_ts replicate_ts;

        get_replicate_ts(&replicate_ts, node, created, updated);
        replicateModify(ctx, replset, argv, &replicate_ts);
    }

    SelvaSubscriptions_SendDeferredEvents(hierarchy);

    return REDISMODULE_OK;
}

/*
 * This might be useful in the future.
 */
#if 0
static int my_RedisModuleEventCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    return 0;
}
    (void)RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, my_RedisModuleEventCallback);
#endif

SELVA_EXPORT int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    int err;

    fprintf(stderr, "Selva version: %s\n", selva_version);

    /* FIXME These pointers end up being NULL */
#if 0
    libdeflate_set_memory_allocator(RedisModule_Alloc, RedisModule_Free);
#endif

    /* Register the module itself */
    if (RedisModule_Init(ctx, "selva", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /*
     * This mode is currently not supported by Selva and should not be enabled
     * as it will just ignore all errors and make Redis crash.
     */
#if 0
    RedisModule_SetModuleOptions(ctx, REDISMODULE_OPTIONS_HANDLE_IO_ERRORS);
#endif

    err = parse_config_args(argv, argc);
    if (err) {
        fprintf(stderr, "%s:%d:%s: Failed to parse config args: %s\n",
                __FILE__, __LINE__, __func__,
                getSelvaErrorStr(err));
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "selva.modify", SelvaCommand_Modify, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    Selva_Onload **onload_p;

    SET_FOREACH(onload_p, selva_onload) {
        Selva_Onload *onload = *onload_p;

        err = onload(ctx);
        if (err) {
            return err;
        }
    }

    return REDISMODULE_OK;
}

/*
 * Here we could use RedisModule_OnUnload() if it was called on exit, but it
 * isn't. Therefore, we use the destructor attribute that is almost always
 * called before the process terminates. As a side note, OnUnload would be never
 * called for Selva because Redis can't unload modules exporting types or
 * something.
 */
__attribute__((destructor))
int _Selva_OnUnload(void) {
    Selva_Onunload **onunload_p;

    SET_FOREACH(onunload_p, selva_onunld) {
        Selva_Onunload *onunload = *onunload_p;

        onunload();
    }

    return REDISMODULE_OK;
}
