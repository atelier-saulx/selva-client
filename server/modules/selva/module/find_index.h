#pragma once
#ifndef _FIND_INDEX_H_
#define _FIND_INDEX_H_

struct RedisModuleCtx;
struct RedisModuleString;
struct SelvaHierarchy;
struct SelvaSet;
struct SelvaFindIndexControlBlock;
struct indexing_timer_args;

int SelvaFindIndex_Init(struct RedisModuleCtx *ctx, struct SelvaHierarchy *hierarchy);
void SelvaFindIndex_Deinit(struct SelvaHierarchy *hierarchy);

/**
 * Check if an index exists for this query, update it, and get the indexing result set.
 * @param out is a SelvaSet of node_ids indexed for given clause.
 */
int SelvaFind_AutoIndex(
        struct RedisModuleCtx *ctx,
        struct SelvaHierarchy *hierarchy,
        enum SelvaTraversal dir, struct RedisModuleString *dir_expression_str,
        const Selva_NodeId node_id,
        struct RedisModuleString *filter,
        struct SelvaFindIndexControlBlock **icb_out,
        struct SelvaSet **out);
void SelvaFind_Acc(struct SelvaFindIndexControlBlock * restrict icb, size_t acc_take, size_t acc_tot);

#endif /* _FIND_INDEX_H_ */
