#pragma once
#ifndef SELVA_MODIFY
#define SELVA_MODIFY

#include <stdbool.h>

enum SelvaModify_ArgType {
  SELVA_MODIFY_ARG_VALUE = '0',
  SELVA_MODIFY_ARG_INDEXED_VALUE = '1',
  SELVA_MODIFY_ARG_DEFAULT = '2',
  SELVA_MODIFY_ARG_DEFAULT_INDEXED = '3',
  SELVA_MODIFY_ARG_OP_INCREMENT = '4',
  SELVA_MODIFY_ARG_OP_SET = '5'
};

struct SelvaModify_OpIncrement {
  int index;
  int $default;
  int $increment;
};

struct SelvaModify_OpSet {
  int is_reference;

  // filled with multiple ids of length 10
  char *$add;
  size_t $add_len;

  // filled with multiple ids of length 10
  char *$delete;
  size_t $delete_len;


  // filled with multiple ids of length 10
  char *$value;
  size_t $value_len;
};

static inline void SelvaModify_OpSet_align(struct SelvaModify_OpSet *op) {
  op->$add = (char *)((char *)op + sizeof(struct SelvaModify_OpSet));
  op->$delete = (char *)((char *)op + sizeof(struct SelvaModify_OpSet) + op->$add_len);
  op->$value = (char *)((char *)op + sizeof(struct SelvaModify_OpSet) + op->$add_len + op->$delete_len);
}

#endif /* SELVA_MODIFY */
