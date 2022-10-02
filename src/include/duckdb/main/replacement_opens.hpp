//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/replacement_open.hpp"

namespace duckdb {

class ExtensionPrefixReplacementOpen : public ReplacementOpen {
public:
	ExtensionPrefixReplacementOpen();
};

} // namespace duckdb
