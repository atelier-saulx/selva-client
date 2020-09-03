#pragma once
#ifndef SELVA_MODIFY_HIERARCHY
#define SELVA_MODIFY_HIERARCHY

#include "linker_set.h"
#include "svector.h"

#define SELVA_NODE_ID_SIZE      10ul
#define SELVA_NODE_TYPE_SIZE    2
#define ROOT_NODE_ID            "root\0\0\0\0\0\0"

/*
 * Error codes.
 */

/**
 * General error.
 */
#define SELVA_MODIFY_HIERARCHY_EGENERAL (-1)
/**
 * Operation not supported.
 */
#define SELVA_MODIFY_HIERARCHY_ENOTSUP  (-2)
/**
 * Invalid argument/input value.
 */
#define SELVA_MODIFY_HIERARCHY_EINVAL   (-3)
/**
 * Out of memory.
 */
#define SELVA_MODIFY_HIERARCHY_ENOMEM   (-4)
/**
 * Node or entity not found.
 */
#define SELVA_MODIFY_HIERARCHY_ENOENT   (-5)
/**
 * Node or entity already exist.
 */
#define SELVA_MODIFY_HIERARCHY_EEXIST   (-6)
/* This must be the last error */


/**
 * Default Redis key name for Selva hierarchy.
 */
#define HIERARCHY_DEFAULT_KEY "___selva_hierarchy"

/**
 * Type for Selva NodeId.
 */
typedef char Selva_NodeId[SELVA_NODE_ID_SIZE];

struct SelvaModify_Hierarchy;
typedef struct SelvaModify_Hierarchy SelvaModify_Hierarchy;

/* Forward declarations for metadata */
/* ... */
/* End of forward declarations for metadata */

/**
 * Hierarchy node metadata.
 * This structure should contain primitive data types or pointers to forward
 * declared structures.
 */
struct SelvaModify_HierarchyMetaData {
    struct SVector subs;
};

typedef void SelvaModify_HierarchyMetadataHook(Selva_NodeId id, struct SelvaModify_HierarchyMetaData *metadata);

#define SELVA_MODIFY_HIERARCHY_METADATA_CONSTRUCTOR(fun) \
    DATA_SET(selva_HMCtor, fun)

#define SELVA_MODIFY_HIERARCHY_METADATA_DESTRUCTOR(fun) \
    DATA_SET(selva_HMDtor, fun)

/**
 * Hierarchy traversal order.
 * Used by SelvaModify_TraverseHierarchy().
 */
enum SelvaModify_HierarchyTraversal {
    SELVA_MODIFY_HIERARCHY_BFS_ANCESTORS,
    SELVA_MODIFY_HIERARCHY_BFS_DESCENDANTS,
    SELVA_MODIFY_HIERARCHY_DFS_ANCESTORS,
    SELVA_MODIFY_HIERARCHY_DFS_DESCENDANTS,
    SELVA_MODIFY_HIERARCHY_DFS_FULL,
};

/**
 * Called for each node found during a traversal.
 * @returns 0 to continue the traversal; 1 to interrupt the traversal.
 */
typedef int (*SelvaModify_HierarchyCallback)(Selva_NodeId id, void *arg, struct SelvaModify_HierarchyMetaData *metadata);

struct SelvaModify_HierarchyCallback {
    SelvaModify_HierarchyCallback node_cb;
    void * node_arg;
};

struct RedisModuleCtx;
struct RedisModuleString;

extern const char * const hierarchyStrError[-SELVA_MODIFY_HIERARCHY_EEXIST + 1];

/**
 * Create a new hierarchy.
 */
SelvaModify_Hierarchy *SelvaModify_NewHierarchy(struct RedisModuleCtx *ctx);

/**
 * Free a hierarchy.
 */
void SelvaModify_DestroyHierarchy(SelvaModify_Hierarchy *hierarchy);

/**
 * Open a hierarchy key.
 */
SelvaModify_Hierarchy *SelvaModify_OpenHierarchy(struct RedisModuleCtx *ctx, struct RedisModuleString *key_name, int mode);

int replyWithHierarchyError(struct RedisModuleCtx *ctx, int err);

int SelvaModify_HierarchyNodeExists(SelvaModify_Hierarchy *hierarchy, const Selva_NodeId id);

ssize_t SelvaModify_GetHierarchyDepth(SelvaModify_Hierarchy *hierarchy, const Selva_NodeId id);

int SelvaModify_DelHierarchyChildren(
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id);

int SelvaModify_DelHierarchyParents(
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id);

/**
 * Set node relationships relative to other existing nodes.
 * Previously existing connections to and from other nodes are be removed.
 * If a node with id doesn't exist it will be created.
 * @param parents   Sets these nodes and only these nodes as parents of this node.
 * @param children  Sets these nodes and only these nodes as children of this node.
 */
int SelvaModify_SetHierarchy(
        struct RedisModuleCtx *ctx,
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id,
        size_t nr_parents,
        const Selva_NodeId *parents,
        size_t nr_children,
        const Selva_NodeId *children);

/**
 * Set parents of an existing node.
 * @param parents   Sets these nodes and only these nodes as parents of this node.
 */
int SelvaModify_SetHierarchyParents(
        struct RedisModuleCtx *ctx,
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id,
        size_t nr_parents,
        const Selva_NodeId *parents);

/**
 * Set children of an existing node.
 * @param children  Sets these nodes and only these nodes as children of this node.
 */
int SelvaModify_SetHierarchyChildren(
        struct RedisModuleCtx *ctx,
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id,
        size_t nr_children,
        const Selva_NodeId *children);

/**
 * Add new relationships relative to other existing nodes.
 * Previously existing connections to and from other nodes are be preserved.
 * If a node with id doesn't exist it will be created.
 * @param parents   Sets these nodes as parents to this node,
 *                  while keeping the existing parents.
 * @param children  Sets these nodes as children to this node,
 *                  while keeping the existing children.
 */
int SelvaModify_AddHierarchy(
        struct RedisModuleCtx *ctx,
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id,
        size_t nr_parents,
        const Selva_NodeId *parents,
        size_t nr_children,
        const Selva_NodeId *children);

/**
 * Remove relationship relative to other existing nodes.
 * @param parents   Removes the child relationship between this node and
 *                  the listed parents.
 * @param children  Removes the parent relationship between this node and
 *                  the listed children.
 */
int SelvaModify_DelHierarchy(
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id,
        size_t nr_parents,
        const Selva_NodeId *parents,
        size_t nr_children,
        const Selva_NodeId *children);

/**
 * Delete a node from the hierarchy.
 */
int SelvaModify_DelHierarchyNode(
        struct RedisModuleCtx *ctx,
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id);

/**
 * Get orphan head nodes of the given hierarchy.
 */
ssize_t SelvaModify_GetHierarchyHeads(SelvaModify_Hierarchy *hierarchy, Selva_NodeId **res);

/**
 * Get an unsorted list of ancestors fo a given node.
 */
ssize_t SelvaModify_FindAncestors(SelvaModify_Hierarchy *hierarchy, const Selva_NodeId id, Selva_NodeId **ancestors);

/**
 * Get an unsorted list of descendants of a given node.
 */
ssize_t SelvaModify_FindDescendants(SelvaModify_Hierarchy *hierarchy, const Selva_NodeId id, Selva_NodeId **descendants);

int SelvaModify_TraverseHierarchy(
        SelvaModify_Hierarchy *hierarchy,
        const Selva_NodeId id,
        enum SelvaModify_HierarchyTraversal dir,
        struct SelvaModify_HierarchyCallback *cb);

/*
 * hierarchy_subscriptions.c
 */
typedef unsigned char Selva_SubscriptionId[32];
enum Selva_SubscriptionType {
    SELVA_SUBSCRIPTION_TYPE_ANCESTORS,
    SELVA_SUBSCRIPTION_TYPE_DESCENDANTS,
};

int SelvaModify_CreateSubscription(
        struct SelvaModify_Hierarchy *hierarchy,
        Selva_SubscriptionId sub_id,
        enum Selva_SubscriptionType type,
        Selva_NodeId node_id);
void SelvaModify_DeleteSubscription(struct SelvaModify_Hierarchy *hierarchy, Selva_SubscriptionId sub_id);
void SelvaModify_ClearAllSubscriptionMarkers(Selva_NodeId id, struct SelvaModify_HierarchyMetaData *metadata);

/*
 * hierarchy_events.c
 */
void SelvaModify_PublishDescendants(struct SelvaModify_Hierarchy *hierarchy, const char *id_str);

#endif /* SELVA_MODIFY_HIERARCHY */
