#pragma once

#include <cstdint>
#include "append_info.hpp"

namespace tpcds {

typedef int64_t ds_key_t;

typedef int (*tpcds_builder_func)(void *, ds_key_t);

void InitializeDSDgen();
ds_key_t GetRowCount(int table_id);
void ResetCountCount();
tpcds_table_def GetTDefByNumber(int table_id);
tpcds_builder_func GetTDefFunctionByNumber(int table_id);

}; // namespace tpcds
