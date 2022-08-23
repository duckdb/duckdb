//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_view_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

struct CreateViewInfo : public CreateInfo {
	CreateViewInfo() : CreateInfo(CatalogType::VIEW_ENTRY, INVALID_SCHEMA) {
	}
	CreateViewInfo(string schema, string view_name)
	    : CreateInfo(CatalogType::VIEW_ENTRY, schema), view_name(view_name) {
	}

	//! Table name to insert to
	string view_name;
	//! Aliases of the view
	vector<string> aliases;
	//! Return types
	vector<LogicalType> types;
	//! The SelectStatement of the view
	unique_ptr<SelectStatement> query;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateViewInfo>(schema, view_name);
		CopyProperties(*result);
		result->aliases = aliases;
		result->types = types;
		result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
		return move(result);
	}

	static unique_ptr<CreateViewInfo> Deserialize(Deserializer &deserializer) {
		auto result = make_unique<CreateViewInfo>();
		result->DeserializeBase(deserializer);

		FieldReader reader(deserializer);
		result->view_name = reader.ReadRequired<string>();
		result->aliases = reader.ReadRequiredList<string>();
		result->types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
		result->query = reader.ReadOptional<SelectStatement>(nullptr);
		reader.Finalize();

		return result;
	}

protected:
	void SerializeInternal(Serializer &serializer) const override {
		FieldWriter writer(serializer);
		writer.WriteString(view_name);
		writer.WriteList<string>(aliases);
		writer.WriteRegularSerializableList(types);
		writer.WriteOptional(query);
		writer.Finalize();
	}
};

} // namespace duckdb
