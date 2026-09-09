#pragma once

#include "append_info-c.hpp"

#include <cstdint>

namespace tpcds {

typedef int64_t ds_key_t;

typedef int (*tpcds_builder_func)(void *, ds_key_t);

void InitializeDSDgen(double scale, int parallel = 1, int child = 1);
ds_key_t GetRowCount(int table_id);
void ResetCountCount();
void SkipTableRows(int table_id, ds_key_t count);
tpcds_table_def GetTDefByNumber(int table_id);
tpcds_builder_func GetTDefFunctionByNumber(int table_id);

}; // namespace tpcds
