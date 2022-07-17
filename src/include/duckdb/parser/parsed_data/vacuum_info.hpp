//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/vacuum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct VacuumOptions {
	bool vacuum;
	bool analyze;
};

struct VacuumInfo : public ParseInfo {
public:
	explicit VacuumInfo(VacuumOptions options) : options(options), has_table(false), table(nullptr) {};

	unique_ptr<VacuumInfo> Copy() {
		auto result = make_unique<VacuumInfo>(options);
		result->has_table = has_table;
		if (has_table) {
			result->ref = ref->Copy();
		}
		return result;
	}

	const VacuumOptions options;

public:
	bool has_table;
	unique_ptr<TableRef> ref;
	TableCatalogEntry *table;
	unordered_map<idx_t, idx_t> column_id_map;
	vector<string> columns;
};

} // namespace duckdb
