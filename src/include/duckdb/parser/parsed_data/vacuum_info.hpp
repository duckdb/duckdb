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
	bool operator==(const VacuumOptions &other) const {
		return other.vacuum == vacuum && other.analyze == analyze;
	}
	bool operator!=(const VacuumOptions &other) const {
		return !(*this == other);
	}
};

struct VacuumInfo : public ParseInfo {
public:
	explicit VacuumInfo(VacuumOptions options) : options(options), has_table(false), table(nullptr) {};
	const VacuumOptions options;
	bool has_table;
	unique_ptr<TableRef> ref;
	TableCatalogEntry *table;
	unordered_map<idx_t, idx_t> column_id_map;
	vector<string> columns;

public:
	unique_ptr<VacuumInfo> Copy() {
		auto result = make_unique<VacuumInfo>(options);
		result->has_table = has_table;
		if (has_table) {
			result->ref = ref->Copy();
		}
		return result;
	}

	bool Equals(const VacuumInfo &other) const {
		if (options != other.options) {
			return false;
		}
		if (has_table != other.has_table) {
			return false;
		}

		if (!ref && !other.ref) {

		} else if (!ref || !other.ref) {
			return false;
		} else if (!ref->Equals(other.ref.get())) {
			return false;
		}

		if (table != other.table) {
			// FIXME: TableCatalogEntry does not have an Equals method, cant check equality here
			throw NotImplementedException("VACUUM_INFO");
		}
		return true;
	}
};

} // namespace duckdb
