/*
 * Copyright (c) 2022 SAULX
 * SPDX-License-Identifier: MIT
 */
#pragma once
#ifndef SELVA_OBJECT
#define SELVA_OBJECT

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include "selva_set.h"
#include "selva_lang.h"

/*
 * Object key types.
 * DO NOT REORDER the numbers as they are used for in the RDB storage format.
 */
enum SelvaObjectType {
    SELVA_OBJECT_NULL = 0,
    SELVA_OBJECT_DOUBLE = 1,
    SELVA_OBJECT_LONGLONG = 2,
    SELVA_OBJECT_STRING = 3,
    SELVA_OBJECT_OBJECT = 4,
    SELVA_OBJECT_SET = 5,
    SELVA_OBJECT_ARRAY = 6,
    SELVA_OBJECT_POINTER = 7,
} __packed;

#define SELVA_OBJECT_REPLY_SPLICE_FLAG  0x01 /*!< Set if the path should be spliced to start from the first wildcard. */
#define SELVA_OBJECT_REPLY_BINUMF_FLAG  0x02 /*!< Send numeric fields in an LE binary format. */
#define SELVA_OBJECT_REPLY_ANY_OBJ_FLAG 0x03 /*!< Send any object as a wildcard reply from SelvaObject_GetPointerPartialMatchStr(). */

/**
 * Size of struct SelvaObject.
 * This must match with the actual size of selva_object.c won't compile.
 */
#define SELVA_OBJECT_BSIZE 304

/**
 * Define a for holding a SelvaObject.
 */
#define STATIC_SELVA_OBJECT(name) _Alignas(void *) char name[SELVA_OBJECT_BSIZE]

struct RedisModuleCtx;
struct RedisModuleIO;
struct RedisModuleKey;
struct RedisModuleString;
struct SVector;
struct SelvaObject;
struct SelvaSet;

typedef uint32_t SelvaObjectMeta_t; /*!< SelvaObject key metadata. */
typedef void SelvaObject_Iterator; /* Opaque type. */
typedef void *(*SelvaObject_PtrLoad)(struct RedisModuleIO *io, int encver, void *data);
typedef void (*SelvaObject_PtrSave)(struct RedisModuleIO *io, void *value, void *data);

struct SelvaObjectPointerOpts {
    /**
     * An unique id for serializing the pointer type.
     * The value 0 is reserved for NOP.
     */
    unsigned ptr_type_id;

    /**
     * Send the pointer value as a reply to the client.
     */
    void (*ptr_reply)(struct RedisModuleCtx *ctx, void *p);

    /**
     * Free a SELVA_OBJECT_POINTER value.
     */
    void (*ptr_free)(void *p);

    /**
     * Get the length or size of a SELVA_OBJECT_POINTER value.
     * The unit of the size is undefined but typically it should be either a
     * count of items or the byte size of the value.
     */
    size_t (*ptr_len)(void *p);

    /**
     * RDB loader for the pointer value.
     */
    SelvaObject_PtrLoad ptr_load;
    /**
     * RDB serializer for the pointer value.
     */
    SelvaObject_PtrSave ptr_save;
};

/*
 * Pointer types.
 * These types are needed for the serialization of opaque pointer types.
 */
#define SELVA_OBJECT_POINTER_EDGE               1
#define SELVA_OBJECT_POINTER_EDGE_CONSTRAINTS   2
#define SELVA_OBJECT_POINTER_LANG               3

/**
 * Register SELVA_OBJECT_POINTER options statically for RDB loading.
 */
#define SELVA_OBJECT_POINTER_OPTS(opts) \
    DATA_SET(selva_objpop, opts)

/**
 * Auto free a SelvaObject when code execution exits a block scope.
 */
#define selvaobject_autofree __attribute__((cleanup(_cleanup_SelvaObject_Destroy)))

struct SelvaObjectAny {
    enum SelvaObjectType type; /*!< Type of the value. */
    enum SelvaObjectType subtype; /*!< Subtype of the value. Arrays use this. */
    SelvaObjectMeta_t user_meta; /*!< User defined metadata. */
    char str_lang[LANG_MAX + 1]; /*!< Language of str if applicable. */
    union {
        double d; /*!< SELVA_OBJECT_DOUBLE */
        long long ll; /*!< SELVA_OBJECT_LONGLONG */
        struct RedisModuleString *str; /* SELVA_OBJECT_STRING */
        struct SelvaObject *obj; /* SELVA_OBJECT_OBJECT */
        struct SelvaSet *set; /*!< SELVA_OBJECT_SET */
        struct SVector *array; /*!< SELVA_OBJECT_ARRAY */
        void *p; /* SELVA_OBJECT_POINTER */
    };
};

/**
 * Create a new SelvaObject.
 * @return Returns a pointer to the newly created object;
 *         In case of OOM a NULL pointer is returned.
 */
struct SelvaObject *SelvaObject_New(void) __attribute__((returns_nonnull, warn_unused_result));
/**
 * Initialize a prealloced buffer as a SelvaObject.
 * The given buffer must be aligned the same way as the struct SelvaObject is
 * aligned.
 */
struct SelvaObject *SelvaObject_Init(char buf[SELVA_OBJECT_BSIZE]) __attribute__((returns_nonnull));
/**
 * Clear all keys in the object, except those listed in exclude.
 */
void SelvaObject_Clear(struct SelvaObject *obj, const char * const exclude[]);
/**
 * Destroy a SelvaObject and free all memory.
 * If the object contains arrays of pointers, the elements pointed won't be
 * freed.
 * If the object contains SELVA_OBJECT_POINTER fields the pointed objects
 * are freed if SelvaObjectPointerOpts is set and ptr_free is set in the ops.
 */
void SelvaObject_Destroy(struct SelvaObject *obj);
void _cleanup_SelvaObject_Destroy(struct SelvaObject **obj);

size_t SelvaObject_MemUsage(const void *value);

/**
 * Delete a key an its value from a SelvaObject.
 */
int SelvaObject_DelKeyStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len);
/**
 * Delete a key an its value from a SelvaObject.
 */
int SelvaObject_DelKey(struct SelvaObject *obj, const struct RedisModuleString *key_name);
/**
 * Check whether a key exists in a SelvaObject.
 */
int SelvaObject_ExistsStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len);
/**
 * Check whether a key exists in a SelvaObject.
 */
int SelvaObject_Exists(struct SelvaObject *obj, const struct RedisModuleString *key_name);
/**
 * Check if the top-level of the given key exists in obj.
 * The part after the first dot doesn't need to exist.
 */
int SelvaObject_ExistsTopLevel(struct SelvaObject *obj, const struct RedisModuleString *key_name);

int SelvaObject_GetDoubleStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, double *out) __attribute__((access(write_only, 4)));
int SelvaObject_GetDouble(struct SelvaObject *obj, const struct RedisModuleString *key_name, double *out) __attribute__((access(write_only, 3)));
int SelvaObject_GetLongLongStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, long long *out) __attribute__((access(write_only, 4)));
int SelvaObject_GetLongLong(struct SelvaObject *obj, const struct RedisModuleString *key_name, long long *out) __attribute__((access(write_only, 3)));
int SelvaObject_GetStringStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, struct RedisModuleString **out) __attribute__((access(write_only, 4)));
int SelvaObject_GetString(struct SelvaObject *obj, const struct RedisModuleString *key_name, struct RedisModuleString **out) __attribute__((access(write_only, 3)));
int SelvaObject_SetDoubleStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, double value);
int SelvaObject_SetDouble(struct SelvaObject *obj, const struct RedisModuleString *key_name, double value);
int SelvaObject_SetDoubleDefaultStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, double value);
int SelvaObject_SetDoubleDefault(struct SelvaObject *obj, const struct RedisModuleString *key_name, double value);
int SelvaObject_UpdateDoubleStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, double value);
int SelvaObject_UpdateDouble(struct SelvaObject *obj, const struct RedisModuleString *key_name, double value);
int SelvaObject_SetLongLongStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, long long value);
int SelvaObject_SetLongLong(struct SelvaObject *obj, const struct RedisModuleString *key_name, long long value);
int SelvaObject_SetLongLongDefaultStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, long long value);
int SelvaObject_SetLongLongDefault(struct SelvaObject *obj, const struct RedisModuleString *key_name, long long value);
int SelvaObject_UpdateLongLongStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, long long value);
/**
 * Update a field value.
 * Return SELVA_EEXIST if the current value equals value; Otherwise set value.
 */
int SelvaObject_UpdateLongLong(struct SelvaObject *obj, const struct RedisModuleString *key_name, long long value);
int SelvaObject_SetStringStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, struct RedisModuleString *value);
/**
 * Set a string value.
 * @param key_name is the name of the key ob obj. The argument is used only for lookup and does't need to be retained.
 * @param value is the value; the caller needs to make sure the string is retained.
 */
int SelvaObject_SetString(struct SelvaObject *obj, const struct RedisModuleString *key_name, struct RedisModuleString *value);

int SelvaObject_IncrementDoubleStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, double default_value, double incr);
int SelvaObject_IncrementDouble(struct SelvaObject *obj, const struct RedisModuleString *key_name, double default_value, double incr);
int SelvaObject_IncrementLongLongStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, long long default_value, long long incr);
int SelvaObject_IncrementLongLong(struct SelvaObject *obj, const struct RedisModuleString *key_name, long long default_value, long long incr);

int SelvaObject_GetObjectStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, struct SelvaObject **out) __attribute__((access(write_only, 4)));
int SelvaObject_GetObject(struct SelvaObject *obj, const struct RedisModuleString *key_name, struct SelvaObject **out) __attribute__((access(write_only, 3)));
int SelvaObject_SetObjectStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, struct SelvaObject *value);
int SelvaObject_SetObject(struct SelvaObject *obj, const struct RedisModuleString *key_name, struct SelvaObject *in);

int SelvaObject_AddDoubleSet(struct SelvaObject *obj, const struct RedisModuleString *key_name, double value);
int SelvaObject_AddLongLongSet(struct SelvaObject *obj, const struct RedisModuleString *key_name, long long value);
int SelvaObject_AddStringSet(struct SelvaObject *obj, const struct RedisModuleString *key_name, struct RedisModuleString *value);
int SelvaObject_RemDoubleSetStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, double value);
int SelvaObject_RemDoubleSet(struct SelvaObject *obj, const struct RedisModuleString *key_name, double value);
int SelvaObject_RemLongLongSetStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, long long value);
int SelvaObject_RemLongLongSet(struct SelvaObject *obj, const struct RedisModuleString *key_name, long long value);
int SelvaObject_RemStringSetStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, struct RedisModuleString *value);
int SelvaObject_RemStringSet(struct SelvaObject *obj, const struct RedisModuleString *key_name, struct RedisModuleString *value);
struct SelvaSet *SelvaObject_GetSetStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len);
struct SelvaSet *SelvaObject_GetSet(struct SelvaObject *obj, const struct RedisModuleString *key_name);

int SelvaObject_AddArrayStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, enum SelvaObjectType subtype, void *p);
int SelvaObject_AddArray(struct SelvaObject *obj, const struct RedisModuleString *key_name, enum SelvaObjectType subtype, void *p);
int SelvaObject_InsertArrayStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, enum SelvaObjectType subtype, void *p);
int SelvaObject_InsertArray(struct SelvaObject *obj, const struct RedisModuleString *key_name, enum SelvaObjectType subtype, void *p);
int SelvaObject_AssignArrayIndexStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, enum SelvaObjectType subtype, size_t idx, void *p);
int SelvaObject_AssignArrayIndex(struct SelvaObject *obj, const struct RedisModuleString *key_name, enum SelvaObjectType subtype, size_t idx, void *p);
int SelvaObject_InsertArrayIndexStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, enum SelvaObjectType subtype, size_t idx, void *p);
int SelvaObject_InsertArrayIndex(struct SelvaObject *obj, const struct RedisModuleString *key_name, enum SelvaObjectType subtype, size_t idx, void *p);
int SelvaObject_RemoveArrayIndexStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, size_t idx);
int SelvaObject_RemoveArrayIndex(struct SelvaObject *obj, const struct RedisModuleString *key_name, size_t idx);
int SelvaObject_GetArrayStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, enum SelvaObjectType *out_subtype, struct SVector **out_p) __attribute__((access(write_only, 5)));
int SelvaObject_GetArray(struct SelvaObject *obj, const struct RedisModuleString *key_name, enum SelvaObjectType *out_subtype, struct SVector **out_p) __attribute__((access(write_only, 4)));
int SelvaObject_GetArrayIndexAsRmsStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, size_t idx, struct RedisModuleString **out) __attribute__((access(write_only, 5)));
int SelvaObject_GetArrayIndexAsSelvaObject(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, size_t idx, struct SelvaObject **out) __attribute__((access(write_only, 5)));
int SelvaObject_GetArrayIndexAsLongLong(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, size_t idx, long long *out) __attribute__((access(write_only, 5)));
int SelvaObject_GetArrayIndexAsDouble(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, size_t idx, double *out) __attribute__((access(write_only, 5)));
size_t SelvaObject_GetArrayLen(struct SelvaObject *obj, const struct RedisModuleString *key_name);
size_t SelvaObject_GetArrayLenStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len);

/**
 * Set a pointer value.
 * @param p is the pointer value and it must be non-NULL.
 * @param opts is an optional pointer to SELVA_OBJECT_POINTER ops that can define
 *             how to free the data pointed by the pointer or how to serialize it.
 */
int SelvaObject_SetPointerStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, void *p, const struct SelvaObjectPointerOpts *opts) __attribute__((access(none, 4)));
/**
 * Set a pointer value.
 * @param p is the pointer value and it must be non-NULL.
 * @param opts is an optional pointer to SELVA_OBJECT_POINTER ops that can define
 *             how to free the data pointed by the pointer or how to serialize it.
 */
int SelvaObject_SetPointer(struct SelvaObject *obj, const struct RedisModuleString *key_name, void *p, const struct SelvaObjectPointerOpts *opts) __attribute__((access(none, 3)));
int SelvaObject_GetPointerStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, void **out_p) __attribute__((access(write_only, 4)));
int SelvaObject_GetPointer(struct SelvaObject *obj, const struct RedisModuleString *key_name, void **out_p) __attribute__((access(write_only, 3)));
/**
 * Get partial match from key_name_str.
 * The length of the matching part is returned.
 * @returns Length of the matching key_name; Otherwise an error code is returned.
 */
int SelvaObject_GetPointerPartialMatchStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, void **out_p) __attribute__((access(write_only, 4)));

int SelvaObject_GetAnyLangStr(struct SelvaObject *obj, struct RedisModuleString *lang, const char *key_name_str, size_t key_name_len, struct SelvaObjectAny *res) __attribute__((access(write_only, 5)));
int SelvaObject_GetAnyLang(struct SelvaObject *obj, struct RedisModuleString *lang, const struct RedisModuleString *key_name, struct SelvaObjectAny *res) __attribute__((access(write_only, 4)));
int SelvaObject_GetAnyStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, struct SelvaObjectAny *res) __attribute__((access(write_only, 4)));
int SelvaObject_GetAny(struct SelvaObject *obj, const struct RedisModuleString *key_name, struct SelvaObjectAny *res) __attribute__((access(write_only, 3)));

enum SelvaObjectType SelvaObject_GetTypeStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len);
enum SelvaObjectType SelvaObject_GetType(struct SelvaObject *obj, const struct RedisModuleString *key_name);

/**
 * Get the length of a SelvaObject or a key value.
 * Return value can be the number of elements or a byte size of the value,
 * depending on the exact type.
 */
ssize_t SelvaObject_LenStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len);
/**
 * Get the length of a SelvaObject or a key value.
 * Return value can be the number of elements or a byte size of the value,
 * depending on the exact type.
 */
ssize_t SelvaObject_Len(struct SelvaObject *obj, const struct RedisModuleString *key_name);

int SelvaObject_GetUserMetaStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, SelvaObjectMeta_t *meta) __attribute__((access(write_only, 4)));
int SelvaObject_GetUserMeta(struct SelvaObject *obj, const struct RedisModuleString *key_name, SelvaObjectMeta_t *meta) __attribute__((access(write_only, 3)));
int SelvaObject_SetUserMetaStr(struct SelvaObject *obj, const char *key_name_str, size_t key_name_len, SelvaObjectMeta_t meta, SelvaObjectMeta_t *old_meta) __attribute__((access(write_only, 5)));
int SelvaObject_SetUserMeta(struct SelvaObject *obj, const struct RedisModuleString *key_name, SelvaObjectMeta_t meta, SelvaObjectMeta_t *old_meta) __attribute__((access(write_only, 4)));

/**
 * @param flags Accepts SELVA_OBJECT_REPLY_SPLICE_FLAG and other reply flags.
 */
int SelvaObject_ReplyWithWildcardStr(
        struct RedisModuleCtx *ctx,
        struct RedisModuleString *lang,
        struct SelvaObject *obj,
        const char *okey_str,
        size_t okey_len,
        long *resp_count,
        int resp_path_start_idx,
        unsigned int flags);

SelvaObject_Iterator *SelvaObject_ForeachBegin(struct SelvaObject *obj);
const char *SelvaObject_ForeachKey(const struct SelvaObject *obj, SelvaObject_Iterator **iterator);

/**
 * Foreach value of specified type in an object.
 * Visiting all subobjects can be achieved by using
 * SelvaObject_ForeachValueType() and recursing when a SELVA_OBJECT_OBJECT is
 * found.
 * @param name_out is a direct pointer to the name and it will be rendered invalid if the key is deleted.
 */
void *SelvaObject_ForeachValue(
        const struct SelvaObject *obj,
        SelvaObject_Iterator **iterator,
        const char **name_out,
        enum SelvaObjectType type);

/**
 * Foreach value in an object.
 * @param name_out is set to the name of the key found.
 * @param type_out is set to the type of the returned value.
 */
void *SelvaObject_ForeachValueType(
        const struct SelvaObject *obj,
        void **iterator,
        const char **name_out,
        enum SelvaObjectType *type_out);

union SelvaObjectArrayForeachValue {
    double d;
    long long ll;
    struct RedisModuleString *rms;
    struct SelvaObject *obj;
};

typedef int (*SelvaObjectArrayForeachCallback)(union SelvaObjectArrayForeachValue value, enum SelvaObjectType subtype, void *arg);

struct SelvaObjectArrayForeachCallback {
    SelvaObjectArrayForeachCallback cb;
    void * cb_arg;
};

/**
 * Foreach value in an array field of an object.
 */
int SelvaObject_ArrayForeach(
        struct SelvaObject *obj,
        const char *field_str,
        size_t field_len,
        const struct SelvaObjectArrayForeachCallback *cb);

union SelvaObjectSetForeachValue {
    struct RedisModuleString *rms;
    double d;
    long long ll;
    Selva_NodeId node_id;
};

typedef int (*SelvaObjectSetForeachCallback)(union SelvaObjectSetForeachValue value, enum SelvaSetType type, void *arg);

struct SelvaObjectSetForeachCallback {
    SelvaObjectSetForeachCallback cb;
    void *cb_arg;
};

/**
 * Foereach value in a set field of an object.
 */
int SelvaObject_SetForeach(
        struct SelvaObject *obj,
        const char *field_str,
        size_t field_len,
        const struct SelvaObjectSetForeachCallback *cb);

/**
 * Get a string name of a SelvaObjectType.
 * @param type is the type.
 * @param len is an optional argument that will be set to the size of the string returned.
 */
const char *SelvaObject_Type2String(enum SelvaObjectType type, size_t *len);

/*
 * Send a SelvaObject as a Redis reply.
 * @param key_name_str can be NULL.
 */
int SelvaObject_ReplyWithObjectStr(
        struct RedisModuleCtx *ctx,
        struct RedisModuleString *lang,
        struct SelvaObject *obj,
        const char *key_name_str,
        size_t key_name_len,
        unsigned flags);

/*
 * Send a SelvaObject as a Redis reply.
 * @param key_name_str can be NULL.
 */
int SelvaObject_ReplyWithObject(
        struct RedisModuleCtx *ctx,
        struct RedisModuleString *lang,
        struct SelvaObject *obj,
        const struct RedisModuleString *key_name,
        unsigned flags);

/**
 * Load a SelvaObject from RDB.
 * @returns a SelvaObject.
 */
struct SelvaObject *SelvaObjectTypeRDBLoad(struct RedisModuleIO *io, int encver, void *ptr_load_data);

struct SelvaObject *SelvaObjectTypeRDBLoadTo(struct RedisModuleIO *io, int encver, struct SelvaObject *obj, void *ptr_load_data);

/**
 * Load a SelvaObject or NULL from RDB.
 * @returns NULL if the object length is zero.
 */
struct SelvaObject *SelvaObjectTypeRDBLoad2(struct RedisModuleIO *io, int encver, void *ptr_load_data);

/**
 * Serialize a SelvaObject in RDB.
 * @param obj is a pointer to the SelvaObject to be serialized.
 * @param ptr_save_data is an optional pointer to additional data that can be used by a registered pointer type.
 */
void SelvaObjectTypeRDBSave(struct RedisModuleIO *io, struct SelvaObject *obj, void *ptr_save_data);

/**
 * Serialize a SelvaObject or NULL.
 * @param obj is a pointer to the SelvaObject to be serialized. Can be NULL.
 * @param ptr_save_data is an optional pointer to additional data that can be used by a registered pointer type.
 */
void SelvaObjectTypeRDBSave2(struct RedisModuleIO *io, struct SelvaObject *obj, void *ptr_save_data);

#endif /* SELVA_OBJECT */
