#include <stdlib.h>
#include <string.h>
#include <tgmath.h>
#include "redismodule.h"
#include "arg_parser.h"
#include "cdefs.h"
#include "errors.h"
#include "subscriptions.h"
#include "selva_object.h"

int SelvaArgParser_IntOpt(ssize_t *value, const char *name, const RedisModuleString *txt, const RedisModuleString *num) {
    TO_STR(txt, num);
    char *end = NULL;

    if (strcmp(name, txt_str)) {
        return SELVA_ENOENT;
    }

    *value = strtoull(num_str, &end, 10);
    if (num_str == end) {
        return SELVA_EINVAL;
    }

    return 0;
}

int SelvaArgParser_StrOpt(const char **value, const char *name, const RedisModuleString *arg_key, const RedisModuleString *arg_val) {
    TO_STR(arg_key, arg_val);

    if (strcmp(name, arg_key_str)) {
        return SELVA_ENOENT;
    }

    if (value) {
        *value = arg_val_str;
    }

    return 0;
}

int SelvaArgsParser_StringList(
        RedisModuleCtx *ctx,
        RedisModuleStringList *out,
        const char *name,
        const RedisModuleString *arg_key,
        const RedisModuleString *arg_val) {
    const char *cur;
    RedisModuleString **list = NULL;
    size_t n = 1;
    int err;

    err = SelvaArgParser_StrOpt(&cur, name, arg_key, arg_val);
    if (err) {
        return err;
    }

    const size_t list_size = n * sizeof(RedisModuleString *);
    list = RedisModule_Realloc(list, list_size);
    if (!list) {
        return SELVA_ENOMEM;
    }

    list[n - 1] = NULL;
    if (cur[0] != '\0') {
        do {
            RedisModuleString *el;
            const char *next;
            size_t len;

            /*
             * Find the separator between the current and the next string.
             */
            next = cur;
            while (*next != '\0' && *next != '\n') {
                next++;
            }
            len = (size_t)((ptrdiff_t)next - (ptrdiff_t)cur);

            /*
             * Create a string.
             */
            el = RedisModule_CreateString(ctx, cur, len);
            if (!el) {
#if MEM_DEBUG
                memset(list, 0, list_size);
#endif
                RedisModule_Free(list);
                return SELVA_ENOMEM;
            }

            /*
             * Set to the array.
             */
            list = RedisModule_Realloc(list, ++n * sizeof(RedisModuleString *));
            list[n - 2] = el;
            list[n - 1] = NULL;

            if (*next == '\0') {
                break;
            }
            cur = next + 1;
        } while (*cur != '\0');
    }

    *out = list;
    return 0;
}

int SelvaArgsParser_StringSetList(
        RedisModuleCtx *ctx,
        struct SelvaObject **list_out,
        RedisModuleString **excluded_out,
        const char *name,
        const RedisModuleString *arg_key,
        const RedisModuleString *arg_val) {
    const char excl_prefix = '!';
    const char set_separator = '\n';
    const char list_separator = '|';
    const char eos = '\0';
    struct SelvaObject *obj;
    RedisModuleString *excl = NULL;
    const char *cur;
    size_t n = 0;
    size_t nr_excl = 0;
    int err;

    err = SelvaArgParser_StrOpt(&cur, name, arg_key, arg_val);
    if (err) {
        return err;
    }

    obj = SelvaObject_New();
    if (!obj) {
        goto fail;
    }

    if (excluded_out) {
        excl = RedisModule_CreateString(ctx, "", 1);
        if (!excl) {
            goto fail;
        }
    }

    if (cur[0] != eos) {
        do {
            const size_t key_len = (size_t)(log10(n + 1)) + 1;
            char key_str[key_len + 1];
            const char *next;
#if 0
            size_t len;
#endif

            snprintf(key_str, key_len + 1, "%zu", n);

            /*
             * Find the separator between the current and the next field name list.
             */
            next = cur;
            while (*next != eos && *next != set_separator) {
                next++;
            }
            /*
             * len could be used for splitting in the do..while loop
             * but we are currently looking for the separator chars
             * there.
             */
#if 0
            len = (size_t)((ptrdiff_t)next - (ptrdiff_t)cur);
#endif

            /*
             * Create the set elements.
             */
            size_t nr_el = 0;
            const char *cur_el = cur;
            do {
                const char *next_el;
                size_t el_len;

                /*
                 * Find the separator between the current and the next field name.
                 */
                next_el = cur_el;
                while (*next_el != eos && *next_el != list_separator && *next_el != set_separator) {
                    next_el++;
                }
                el_len = (size_t)((ptrdiff_t)next_el - (ptrdiff_t)cur_el);

                if (el_len > 0) { /* Skip empty elements. */
                    if (excl && cur_el[0] == excl_prefix) {
                        if (el_len > 1) {
                            if (RedisModule_StringAppendBuffer(ctx, excl, cur_el + 1, el_len - 1) ||
                                RedisModule_StringAppendBuffer(ctx, excl, "", 1)) {
                                goto fail;
                            }
                            nr_excl++;
                        }
                        /* Otherwise we ignore the empty element. */
                    } else {
                        RedisModuleString *el;

                        el = RedisModule_CreateString(NULL, cur_el, el_len);
                        if (!el) {
                            goto fail;
                        }

                        /*
                         * Add to the list.
                         */
                        SelvaObject_AddArrayStr(obj, key_str, key_len, SELVA_OBJECT_STRING, el);
                        nr_el++;
                    }
                }

                if (*next_el == eos || *next_el == set_separator) {
                    break;
                }
                cur_el = next_el + 1;
            } while (*cur_el != eos);

            /*
             * Increment the set index only if elements were inserted at the
             * index.
             */
            if (nr_el > 0) {
                n++;
            }

            if (*next == eos) {
                break;
            }
            cur = next + 1;
        } while (*cur != eos);
    }

    *list_out = obj;
    if (excluded_out && nr_excl > 0) {
        *excluded_out = excl;
    } else if (excluded_out) {
        *excluded_out = NULL;
        RedisModule_FreeString(ctx, excl);
    } else {
        RedisModule_FreeString(ctx, excl);
    }
    return 0;
fail:
    if (obj) {
        SelvaObject_Destroy(obj);
    }
    if (excl) {
        RedisModule_FreeString(ctx, excl);
    }
    return SELVA_ENOMEM;
}

int SelvaArgParser_Enum(
        const struct SelvaArgParser_EnumType types[],
        const RedisModuleString *arg) {
    size_t i = 0;
    TO_STR(arg);

    while (types[i].name) {
        if (!strcmp(types[i].name, arg_str)) {
            return types[i].id;
        }
        i++;
    }

    return SELVA_ENOENT;
}

int SelvaArgParser_NodeId(Selva_NodeId node_id, const RedisModuleString *arg) {
    size_t len;
    const char *str;

    str = RedisModule_StringPtrLen(arg, &len);

    if (len < SELVA_NODE_TYPE_SIZE + 1) {
        return SELVA_EINVAL;
    }

    memset(node_id, 0, SELVA_NODE_ID_SIZE);
    memcpy(node_id, str, min(SELVA_NODE_ID_SIZE, len));

    return 0;
}

int SelvaArgParser_NodeType(Selva_NodeType node_type, const RedisModuleString *arg) {
    size_t len;
    const char *str;

    str = RedisModule_StringPtrLen(arg, &len);

    if (len < SELVA_NODE_TYPE_SIZE) {
        return SELVA_EINVAL;
    }

    memcpy(node_type, str, sizeof(Selva_NodeType));
    return 0;
}

int SelvaArgParser_SubscriptionId(Selva_SubscriptionId id, const RedisModuleString *arg) {
    TO_STR(arg);

    if (arg_len != SELVA_SUBSCRIPTION_ID_STR_LEN) {
        return SELVA_SUBSCRIPTIONS_EINVAL;
    }

    return Selva_SubscriptionStr2id(id, arg_str);
}

int SelvaArgParser_MarkerId(Selva_SubscriptionMarkerId *marker_id, const RedisModuleString *arg) {
    long long ll;

    if (RedisModule_StringToLongLong(arg, &ll) != REDISMODULE_OK) {
        return SELVA_SUBSCRIPTIONS_EINVAL;
    }

    *marker_id = (Selva_SubscriptionMarkerId)ll;
    return 0;
}

int SelvaArgParser_IndexHints(RedisModuleStringList *out, RedisModuleString **argv, int argc) {
    RedisModuleString **list = NULL;
    int n = 0;

    for (int i = 0; i < argc; i += 2) {
        RedisModuleString **new_list;

        if (i + 1 >= argc || strcmp("index", RedisModule_StringPtrLen(argv[i], NULL))) {
            break;
        }

        const size_t list_size = ++n * sizeof(RedisModuleString *);
        new_list = RedisModule_Realloc(list, list_size);
        if (!new_list) {
            RedisModule_Free(list);
            return SELVA_ENOMEM;
        }

        list = new_list;
        list[n - 1] = argv[i + 1];
    }

    *out = list;
    return n;
}
