//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/exported_table_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class TableCatalogEntry;

struct ExportedTableData {
	//! The qualified name of the exported table (database = catalog, schema, table = name)
	QualifiedName qualified_name;

	void SetQualifiedName(Identifier catalog, Identifier schema, Identifier name) {
		qualified_name = QualifiedName(std::move(catalog), std::move(schema), std::move(name));
	}

	//! Path to be exported
	string file_path;
	//! Not Null columns, if any
	vector<Identifier> not_null_columns;

	void Serialize(Serializer &serializer) const;
	static ExportedTableData Deserialize(Deserializer &deserializer);
};

struct ExportedTableInfo {
	ExportedTableInfo(TableCatalogEntry &entry, ExportedTableData table_data_p, vector<Identifier> &not_null_columns_p);
	ExportedTableInfo(ClientContext &context, ExportedTableData table_data);

	TableCatalogEntry &entry;
	ExportedTableData table_data;

	void Serialize(Serializer &serializer) const;
	static ExportedTableInfo Deserialize(Deserializer &deserializer);

private:
	static TableCatalogEntry &GetEntry(ClientContext &context, const ExportedTableData &table_data);
};

struct BoundExportData : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::BOUND_EXPORT_DATA;

public:
	BoundExportData() : ParseInfo(TYPE) {
	}

	vector<ExportedTableInfo> data;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
