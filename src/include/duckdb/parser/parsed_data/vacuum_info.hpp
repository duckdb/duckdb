//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/vacuum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct VacuumInfo : public ParseInfo {
public:
	VacuumInfo() {};

	unique_ptr<VacuumInfo> Copy() {
		auto result = make_unique<VacuumInfo>();
		result->table_ref = table_ref->Copy();
		result->table = table;
		return result;
	}

public:
	unique_ptr<TableRef> table_ref;
	TableCatalogEntry *table;
};

} // namespace duckdb
