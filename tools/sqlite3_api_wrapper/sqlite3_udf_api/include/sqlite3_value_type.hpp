#pragma once

namespace duckdb {
enum class SQLiteTypeValue : uint8_t { INTEGER = 1, FLOAT = 2, TEXT = 3, BLOB = 4, NULL_VALUE = 5 };
}
