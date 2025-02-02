/*
 * Copyright (c) 2020-2022 SAULX
 * SPDX-License-Identifier: MIT
 */
#include <stdint.h>
#include "trx.h"

int Trx_Begin(struct trx_state * restrict state, struct trx * restrict trx) {
    const trxid_t cl = (trxid_t)1 << __builtin_popcountl(state->cl);

    if (cl == (trxid_t)1 << (sizeof(trxid_t) * 8 - 1)) {
        return -1;
    }

    state->cl |= cl;

    trx->id = state->id;
    trx->cl = cl;

    return 0;
}

void Trx_Sync(const struct trx_state * restrict state, struct trx * restrict label) {
    if (label->id != state->id) {
        label->id = state->id;
        label->cl = 0; /* Not visited yet. */
    }
}

int Trx_Visit(struct trx * restrict cur_trx, struct trx * restrict label) {
    if (cur_trx->id != label->id) {
        /* Visit & first visit of this trx. */
        label->id = cur_trx->id;
        label->cl = cur_trx->cl;

        return 1;
    } else if (!(cur_trx->cl & label->cl)) {
        /* Visit */
        label->cl |= cur_trx->cl;

        return 1;
    }

    return 0; /* Don't visit. */
}

int Trx_HasVisited(const struct trx * restrict cur_trx, const struct trx * restrict label) {
    return label->id == cur_trx->id && (label->cl & cur_trx->cl) == cur_trx->cl;
}

void Trx_End(struct trx_state * restrict state, struct trx * restrict cur) {
    state->ex |= cur->cl;

    if (state->ex == state->cl) {
        state->id++; /* Increment id for the next traversal. */
        state->cl = 0;
        state->ex = 0;
    }
}
