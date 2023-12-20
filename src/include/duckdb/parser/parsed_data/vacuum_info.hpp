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
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

struct VacuumOptions {
	VacuumOptions() : vacuum(false), analyze(false) {
	}

	bool vacuum;
	bool analyze;

	void Serialize(Serializer &serializer) const;
	static VacuumOptions Deserialize(Deserializer &deserializer);
};

struct VacuumInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::VACUUM_INFO;

public:
	explicit VacuumInfo(VacuumOptions options);

	const VacuumOptions options;

public:
	bool has_table;
	unique_ptr<TableRef> ref;
	optional_ptr<TableCatalogEntry> table;
	unordered_map<idx_t, idx_t> column_id_map;
	vector<string> columns;

public:
	unique_ptr<VacuumInfo> Copy();

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
