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

namespace duckdb {
class Serializer;
class Deserializer;

struct VacuumOptions {
	VacuumOptions() : vacuum(false), analyze(false) {
	}

	bool vacuum;
	bool analyze;
	//! SereneDB-specific pragma option: a REFRESH_*/COMPACT_*/RECOMPUTE_STATS_* scope
	//! verb. Lowers VACUUM to `PRAGMA serenedb_vacuum(<this>, name, [schema], [catalog])`
	//! instead of producing a VacuumStatement.
	string serenedb_pragma_option;

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
