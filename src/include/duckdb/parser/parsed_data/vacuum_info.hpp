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

namespace duckdb {

enum class VacuumOptions : int { VACUUM = 1 << 0, ANALYZE = 1 << 1 };

struct VacuumInfo : public ParseInfo {
public:
	explicit VacuumInfo(const VacuumOptions &options) : options(options), has_table(false) {};

	unique_ptr<VacuumInfo> Copy() {
		auto result = make_unique<VacuumInfo>(options);
		result->has_table = has_table;
		if (has_table) {
			result->ref = ref->Copy();
		}
		return result;
	}

	bool Vacuum() {
		return HasOption(VacuumOptions::VACUUM);
	}

	bool Analyze() {
		return HasOption(VacuumOptions::ANALYZE);
	}

public:
	//! Bitmap of options set by the parser
	const VacuumOptions options;

	bool has_table;
	unique_ptr<TableRef> ref;
	unique_ptr<BoundBaseTableRef> bound_ref;
	vector<string> columns;

private:
	bool HasOption(const VacuumOptions &option) {
		return int(options) & int(option);
	}
};

} // namespace duckdb
