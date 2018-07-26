
#pragma once

#include <memory.h>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

namespace duckdb {

int32_t Hash(bool integer);
int32_t Hash(int8_t integer);
int32_t Hash(int16_t integer);
int32_t Hash(int32_t integer);
int32_t Hash(int64_t integer);
int32_t Hash(double integer);
int32_t Hash(std::string str);
}
