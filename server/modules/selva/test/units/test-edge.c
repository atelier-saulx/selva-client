#include <punit.h>
#include <stdlib.h>
#include "hierarchy.h"
#include "edge.h"
#include "../hierarchy-utils.h"

static void setup(void)
{
    hierarchy = SelvaModify_NewHierarchy(NULL);
}

static void teardown(void)
{
    SelvaModify_DestroyHierarchy(hierarchy);
    hierarchy = NULL;

    free(findRes);
    findRes = NULL;
}

static char * test_alter_edge_relationship(void)
{
    /*
     *  a.a ---> c
     *  b.a ---> d
     * =>
     *  a.a ----> c
     *  b.a --/
     *        \> d
     */

    /* Create nodes. */
    SelvaModify_SetHierarchy(NULL, hierarchy, "grphnode_a", 0, NULL, 0, NULL, NULL);
    SelvaModify_SetHierarchy(NULL, hierarchy, "grphnode_b", 0, NULL, 0, NULL, NULL);
    SelvaModify_SetHierarchy(NULL, hierarchy, "grphnode_c", 0, NULL, 0, NULL, NULL);
    SelvaModify_SetHierarchy(NULL, hierarchy, "grphnode_d", 0, NULL, 0, NULL, NULL);

    /* Add edges. */
    pu_assert("Add edge", !Edge_Add(NULL, hierarchy, 0, "a", 1, SelvaHierarchy_FindNode(hierarchy, "grphnode_a"), SelvaHierarchy_FindNode(hierarchy, "grphnode_c")));
    pu_assert("Add edge", !Edge_Add(NULL, hierarchy, 0, "a", 1, SelvaHierarchy_FindNode(hierarchy, "grphnode_b"), SelvaHierarchy_FindNode(hierarchy, "grphnode_d")));

    pu_assert_equal("a.a has c", Edge_Has(Edge_GetField(SelvaHierarchy_FindNode(hierarchy, "grphnode_a"), "a", 1), SelvaHierarchy_FindNode(hierarchy, "grphnode_c")), 1);
    pu_assert_equal("b.a has d", Edge_Has(Edge_GetField(SelvaHierarchy_FindNode(hierarchy, "grphnode_b"), "a", 1), SelvaHierarchy_FindNode(hierarchy, "grphnode_d")), 1);

    /* Alter edges. */
    pu_assert("Add edge", !Edge_Add(NULL, hierarchy, 0, "a", 1, SelvaHierarchy_FindNode(hierarchy, "grphnode_b"), SelvaHierarchy_FindNode(hierarchy, "grphnode_c")));

    pu_assert_equal("a.a has c", Edge_Has(Edge_GetField(SelvaHierarchy_FindNode(hierarchy, "grphnode_a"), "a", 1), SelvaHierarchy_FindNode(hierarchy, "grphnode_c")), 1);
    pu_assert_equal("b.a has c", Edge_Has(Edge_GetField(SelvaHierarchy_FindNode(hierarchy, "grphnode_b"), "a", 1), SelvaHierarchy_FindNode(hierarchy, "grphnode_c")), 1);
    pu_assert_equal("b.a has d", Edge_Has(Edge_GetField(SelvaHierarchy_FindNode(hierarchy, "grphnode_b"), "a", 1), SelvaHierarchy_FindNode(hierarchy, "grphnode_d")), 1);

    return NULL;
}

static char * test_delete_edge(void)
{
    /* Create nodes. */
    SelvaModify_SetHierarchy(NULL, hierarchy, "grphnode_a", 0, NULL, 0, NULL, NULL);
    SelvaModify_SetHierarchy(NULL, hierarchy, "grphnode_b", 0, NULL, 0, NULL, NULL);
    SelvaModify_SetHierarchy(NULL, hierarchy, "grphnode_c", 0, NULL, 0, NULL, NULL);

    /* Add edges. */
    pu_assert("Add edge", !Edge_Add(NULL, hierarchy, 0, "a", 1, SelvaHierarchy_FindNode(hierarchy, "grphnode_a"), SelvaHierarchy_FindNode(hierarchy, "grphnode_b")));
    pu_assert("Add edge", !Edge_Add(NULL, hierarchy, 0, "a", 1, SelvaHierarchy_FindNode(hierarchy, "grphnode_a"), SelvaHierarchy_FindNode(hierarchy, "grphnode_c")));

    /* Delete an edge. */
    struct SelvaHierarchyNode *node_a = SelvaHierarchy_FindNode(hierarchy, "grphnode_a");
    Edge_Delete(NULL, NULL, Edge_GetField(node_a, "a", 1), node_a, "grphnode_b");

    pu_assert_equal("a.a doesn't have b", Edge_Has(Edge_GetField(SelvaHierarchy_FindNode(hierarchy, "grphnode_a"), "a", 1), SelvaHierarchy_FindNode(hierarchy, "grphnode_b")), 0);
    pu_assert_equal("b.a has c", Edge_Has(Edge_GetField(SelvaHierarchy_FindNode(hierarchy, "grphnode_a"), "a", 1), SelvaHierarchy_FindNode(hierarchy, "grphnode_c")), 1);

    return NULL;
}

void all_tests(void)
{
    pu_def_test(test_alter_edge_relationship, PU_RUN);
    pu_def_test(test_delete_edge, PU_RUN);
}
