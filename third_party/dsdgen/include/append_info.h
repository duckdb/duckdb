#ifndef R_APPEND_H
#define R_APPEND_H

#include <stdbool.h>
#include <stdlib.h>

#include "decimal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *append_info;

void append_row(append_info *info);

void append_varchar(append_info *info, const char *value);
void append_key(append_info *info, int64_t value);
void append_date(append_info *info, int64_t value);
void append_integer(append_info *info, int32_t value);
void append_decimal(append_info *info, decimal_t *val);

#ifdef __cplusplus
};
#endif

#endif
