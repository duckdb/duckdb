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
#include "duckdb/catalog/dependency_list.hpp"

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
	vector<string> columns;
	bool has_table;
	unique_ptr<TableRef> ref;

public:
	unique_ptr<VacuumInfo> Copy();
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
