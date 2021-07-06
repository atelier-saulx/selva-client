#pragma once
#ifndef SELVA_TRAVERSAL
#define SELVA_TRAVERSAL

#include "svector.h"
#include "arg_parser.h"

enum SelvaTraversalAlgo {
    HIERARCHY_BFS,
    HIERARCHY_DFS,
};

/**
 * Hierarchy traversal order.
 * Recognized by SelvaModify_TraverseHierarchy().
 */
enum SelvaTraversal {
    SELVA_HIERARCHY_TRAVERSAL_NONE, /*!< Do nothing. */
    SELVA_HIERARCHY_TRAVERSAL_NODE, /*!< Visit just the given node. */
    SELVA_HIERARCHY_TRAVERSAL_CHILDREN, /*!< Visit children of the given node. */
    SELVA_HIERARCHY_TRAVERSAL_PARENTS, /*!< Visit parents of the given node. */
    SELVA_HIERARCHY_TRAVERSAL_BFS_ANCESTORS, /*!< Visit ancestors of the given node using BFS. */
    SELVA_HIERARCHY_TRAVERSAL_BFS_DESCENDANTS, /*!< Visit descendants of the given node using BFS. */
    SELVA_HIERARCHY_TRAVERSAL_DFS_ANCESTORS, /*!< Visit ancestors of the given node using DFS. */
    SELVA_HIERARCHY_TRAVERSAL_DFS_DESCENDANTS, /*!< Visit descendants of the given node using DFS. */
    SELVA_HIERARCHY_TRAVERSAL_DFS_FULL, /*!< Full DFS traversal of the whole hierarchy. */
    SELVA_HIERARCHY_TRAVERSAL_REF, /*!< Visit nodes pointed by a ref field. */
    SELVA_HIERARCHY_TRAVERSAL_ARRAY, /*!< Visit nodes pointed by a ref field. */
    SELVA_HIERARCHY_TRAVERSAL_BFS_EDGE_FIELD, /*!< Traverse an edge field according to its constraints using BFS. */
    SELVA_HIERARCHY_TRAVERSAL_BFS_EXPRESSION, /*!< Traverse with an expression returning a set of field names. */
};

enum SelvaMergeStrategy {
    MERGE_STRATEGY_NONE = 0, /* No merge. */
    MERGE_STRATEGY_ALL,
    MERGE_STRATEGY_NAMED,
    MERGE_STRATEGY_DEEP,
};

enum FindCommand_OrderedItemType {
    ORDERED_ITEM_TYPE_EMPTY,
    ORDERED_ITEM_TYPE_TEXT,
    ORDERED_ITEM_TYPE_DOUBLE,
};

struct FindCommand_OrderedItem {
    Selva_NodeId id;
    enum FindCommand_OrderedItemType type;
    struct SelvaObject *data_obj;
    double d;
    size_t data_len;
    char data[];
};

enum SelvaResultOrder {
    HIERARCHY_RESULT_ORDER_NONE,
    HIERARCHY_RESULT_ORDER_ASC,
    HIERARCHY_RESULT_ORDER_DESC,
};

typedef int (*orderFunc)(const void ** restrict a_raw, const void ** restrict b_raw);

struct RedisModuleString;
struct RedisModuleCtx;
struct SelvaModify_Hierarchy;
struct SelvaModify_HierarchyNode;

extern const struct SelvaArgParser_EnumType merge_types[3];

struct FindCommand_Args {
    struct RedisModuleCtx *ctx;
    struct RedisModuleString *lang;
    struct SelvaModify_Hierarchy *hierarchy;

    ssize_t *nr_nodes; /*!< Number of nodes in the result. */
    ssize_t offset; /*!< Start from nth node. */
    ssize_t *limit; /*!< Limit the number of result. */

    struct rpn_ctx *rpn_ctx;
    const struct rpn_expression *filter;

    enum SelvaMergeStrategy merge_strategy;
    struct RedisModuleString *merge_path;
    size_t *merge_nr_fields;

    /**
     * Field names.
     * If set the callback should return the value of these fields instead of
     * node IDs.
     *
     * fields selected in cmd args:
     * ```
     * {
     *   '0': ['field1', 'field2'],
     *   '1': ['field3', 'field4'],
     * }
     * ```
     *
     * merge && no fields selected in cmd args:
     * {
     * }
     *
     * and the final callback will use this as a scratch space to mark which
     * fields have been already sent.
     */
    struct SelvaObject *fields;

    const struct RedisModuleString *order_field; /*!< Order by field name; Otherwise NULL. */
    SVector *order_result; /*!< Results of the find wrapped in FindCommand_OrderedItem structs. Only used if sorting is requested. */

    struct Selva_SubscriptionMarker *marker; /*!< Used by FindInSub. */
};

int SelvaTraversal_ParseOrder(
        const struct RedisModuleString **order_by_field,
        enum SelvaResultOrder *order,
        const struct RedisModuleString *txt,
        const struct RedisModuleString *fld,
        const struct RedisModuleString *ord);
int SelvaTraversal_ParseDir(
        struct RedisModuleCtx *ctx,
        struct SelvaModify_Hierarchy *hierarchy,
        enum SelvaTraversal *dir,
        struct RedisModuleString **field_name_out,
        Selva_NodeId nodeId,
        enum SelvaTraversalAlgo algo,
        const struct RedisModuleString *field_name);
orderFunc SelvaTraversal_GetOrderFunc(enum SelvaResultOrder order);
struct FindCommand_OrderedItem *SelvaTraversal_CreateOrderItem(
        struct RedisModuleCtx *ctx,
        struct RedisModuleString *lang,
        struct SelvaModify_HierarchyNode *node,
        const struct RedisModuleString *order_field);
struct FindCommand_OrderedItem *SelvaTraversal_CreateObjectBasedOrderItem(
        struct RedisModuleCtx *ctx,
        struct RedisModuleString *lang,
        struct SelvaObject *obj,
        const struct RedisModuleString *order_field);
int SelvaTraversal_FieldsContains(struct SelvaObject *fields, const char *field_name_str, size_t field_name_len);
int SelvaTraversal_GetSkip(enum SelvaTraversal dir);

#endif /* SELVA_TRAVERSAL */
