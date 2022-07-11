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
	explicit VacuumInfo(const int &options) : options(options), has_table(false) {};

	unique_ptr<VacuumInfo> Copy() {
		auto result = make_unique<VacuumInfo>(options);
		result->has_table = has_table;
		if (has_table) {
			result->ref = ref->Copy();
		}
		return result;
	}

	bool Vacuum() {
		return options & VACOPT_VACUUM;
	}

	bool Analyze() {
		return options & VACOPT_ANALYZE;
	}

	bool Verbose() {
		return options & VACOPT_VERBOSE;
	}

	bool Freeze() {
		return options & VACOPT_FREEZE;
	}

	bool Full() {
		return options & VACOPT_FULL;
	}

	bool NoWait() {
		return options & VACOPT_NOWAIT;
	}

	bool SkipToast() {
		return options & VACOPT_SKIPTOAST;
	}

	bool DisablePageSkipping() {
		return options & VACOPT_DISABLE_PAGE_SKIPPING;
	}

public:
	const int options;

	bool has_table;
	unique_ptr<TableRef> ref;
	unique_ptr<BoundBaseTableRef> bound_ref;
	vector<string> columns;

private:
	//! Same as postgres vacuum options
	constexpr static const int VACOPT_VACUUM = 1 << 0;                /* do VACUUM */
	constexpr static const int VACOPT_ANALYZE = 1 << 1;               /* do ANALYZE */
	constexpr static const int VACOPT_VERBOSE = 1 << 2;               /* print progress info */
	constexpr static const int VACOPT_FREEZE = 1 << 3;                /* FREEZE option */
	constexpr static const int VACOPT_FULL = 1 << 4;                  /* FULL (non-concurrent) vacuum */
	constexpr static const int VACOPT_NOWAIT = 1 << 5;                /* don't wait to get lock (autovacuum only) */
	constexpr static const int VACOPT_SKIPTOAST = 1 << 6;             /* don't process the TOAST table; if any */
	constexpr static const int VACOPT_DISABLE_PAGE_SKIPPING = 1 << 7; /* don't skip any pages */
};

} // namespace duckdb
