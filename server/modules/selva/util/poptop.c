#include <stddef.h>
#include <string.h>
#include "redismodule.h"

struct poptop_list_el {
    int score;
    void *p;
};

struct poptop {
    /**
     * Maximum size of the top list.
     */
    size_t max_size;
    /**
     * Minimum score.
     * A long term minimum score for an element to get into and remain in the
     * data structure. This is updated when poptop_maintenance() is called.
     */
    int cut_limit;
    struct poptop_list_el *list;
};

struct poptop_loc {
    struct poptop_list_el *found;
    struct poptop_list_el *next_free;
};

/**
 * Initialize a poptop structure.
 * @param l is a pointer to an uninitalized poptop structure.
 * @param max_size is the maximum number of elements allowed in the data
 * structure. Poptop will attempt to maintain a list of about half of the
 * maximum size.
 * @param initial_cut is an initial value for the cut limit.
 * @returns 0 if succeed; Otherwise a non-zero value is returned.
 */
int poptop_init(struct poptop *l, size_t max_size, int initial_cut) {
    l->max_size = max_size;
    l->cut_limit = initial_cut;
    l->list = RedisModule_Calloc(max_size, sizeof(struct poptop_list_el));

    return !l->list;
}

/**
 * Deinit a poptop structure.
 * @param l is a pointer to an initialized poptop structure.
 */
void poptop_deinit(struct poptop *l) {
    RedisModule_Free(l->list);
    memset(l, 0, sizeof(*l));
}

static struct poptop_loc poptop_find(struct poptop * restrict l, void * restrict p) {
    size_t n = l->max_size;
    void *list = l->list;
    struct poptop_list_el *found = NULL;
    struct poptop_list_el *next_free = NULL;

    for (size_t i = 0; i < n; i++) {
        struct poptop_list_el *el = &list[i];

        if (!el->p) {
            next_free = el;
        } else if (el->p == p) {
            found = el;
            break;
        }
    }

    return (struct poptop_loc){
        .found = found,
        .next_free = next_free,
    };
}

/**
 * Add an element to the top list if it's above the self-determined score limit.
 * @param l is a pointer to the poptop structure.
 */
void poptop_maybe_add(struct poptop * restrict l, int score, void * restrict p) {
    struct poptop_loc loc = poptop_find(l, p);

    if (loc.found) {
        /* Already inserted. */
        loc.found->score = score;
    } else if (loc.next_free && score >= l->cut_limit) {
        loc.next_free->score = score;
        loc.next_free->p = p;
    }
}

/**
 * Remove an element from the top list.
 * @param l is a pointer to the poptop structure.
 */
void poptop_remove(struct poptop * restrict l, void * restrict p) {
    struct poptop_loc loc = poptop_find(l, p);

    if (loc.found) {
        loc.found->p = NULL;
    }
}

static int poptop_list_el_compare(const void *a, const void *b) {
    const struct poptop_list_el *el_a = (const struct poptop_list_el *)a;
    const struct poptop_list_el *el_b = (const struct poptop_list_el *)b;

    if (!el_a->p && !el_b->p) {
        return 0;
    } else if (!el_a->p) {
        return 1;
    } else if (!el_b->p) {
        return -1;
    } else {
        return el_a->score - el_b->score;
    }
}

/**
 * Sort the poptop list in l.
 */
static inline void poptop_sort(struct poptop *l) {
    qsort(l->list, l->max_size, sizeof(*l->list), poptop_list_el_compare);
}

/**
 * Find the last element in a sort poptop list.
 */
static size_t poptop_find_last(const struct poptop *l) {
    struct poptop_list_el *list = l->list;
    size_t n = l->max_size;
    size_t i = 0;

    while (i < n && list[i].p) i++;

    return i - 1;
}

/**
 * Get the median score from a sorted poptop list.
 */
static int poptop_median_score(const struct poptop *l, size_t last) {
    int median;

    if (last & 1) { /* The number of elements (last + 1) is even. */
        size_t i = (last + 1) / 2;

        median = (l->list[i].score + l->list[i - 1].score) / 2;
    } else { /* The number of elements is odd. */
        median = l->list[last / 2].score;
    }

    return median;
}

/**
 * Periodic maintenance.
 * Find the median score and establish a new cut limit.
 * @param l is a pointer to the poptop structure.
 */
void poptop_maintenance(struct poptop *l) {
    poptop_sort(l);
    l->cut_limit = poptop_median_score(l, poptop_find_last(l));
}

/**
 * Drop an element that has a score below the cut limit.
 * This function should be called repeatedly until no entry is returned.
 * @param l is a pointer to the poptop structure.
 * @returns a pointer to the element that was removed from the data structure l.
 */
void *poptop_maintenance_drop(struct poptop *l) {
    struct poptop_list_el *list = l->list;
    size_t n = l->max_size;

    for (size_t i = 0; i < n; i++) {
        struct poptop_list_el *el = &list[i];

        if (el->p && el->score < l->cut_limit) {
            void *p;

            p = el->p;
            el->p = NULL;

            return p;
        }
    }

    return NULL;
}
