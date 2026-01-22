//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {
class Deserializer;

//! The TableFilterExtensionInfo holds static information relevant to the table filter extension
struct TableFilterExtensionInfo {
	virtual ~TableFilterExtensionInfo() {
	}
};

class TableFilterExtension {
public:
	virtual ~TableFilterExtension() {
	}

	virtual string GetName() = 0;
	virtual unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) = 0;

	//! Additional info passed to the Deserialize function
	shared_ptr<TableFilterExtensionInfo> table_filter_info;
};

} // namespace duckdb
