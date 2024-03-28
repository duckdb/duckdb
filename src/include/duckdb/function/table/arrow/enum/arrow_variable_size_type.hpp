#pragma once

namespace duckdb {

//===--------------------------------------------------------------------===//
// Arrow Variable Size Types
//===--------------------------------------------------------------------===//
enum class ArrowVariableSizeType : uint8_t { NORMAL, FIXED_SIZE, SUPER_SIZE, VIEW };

} // namespace duckdb
