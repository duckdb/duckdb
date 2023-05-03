#ifndef R_APPEND_H
#define R_APPEND_H

#include <stdbool.h>
#include <stdlib.h>

#include "decimal.h"

typedef void *append_info;

append_info *append_info_get(void *info_list, int table_id);

void append_row_start(append_info info);
void append_row_end(append_info info);

void append_varchar(append_info info, const char *value);
void append_key(append_info info, int64_t value);
void append_date(append_info info, int64_t value);
void append_integer(append_info info, int32_t value);
void append_decimal(append_info info, decimal_t *val);
void append_integer_decimal(append_info info, int32_t val);
void append_boolean(append_info info, int32_t val);

#endif
