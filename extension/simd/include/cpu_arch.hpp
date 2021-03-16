#include "cstdint"
#include "duckdb/common/string.hpp"

namespace duckdb {
//===--------------------------------------------------------------------===//
// Instruction Sets
//===--------------------------------------------------------------------===//
enum class Architecture : uint8_t { FALLBACK = 0, X86 = 1, X86_64 = 2, ARM = 4 };

string ArchitectureToString(Architecture arch);
} // namespace duckdb
