//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/vacuum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

struct VacuumInfo : public ParseInfo {
public:
	VacuumInfo() {};

	unique_ptr<VacuumInfo> Copy() {
		auto result = make_unique<VacuumInfo>();
		result->ref = ref->Copy();
		return result;
	}

public:
	unique_ptr<TableRef> ref;
	unique_ptr<BoundBaseTableRef> bound_ref;
};

} // namespace duckdb
