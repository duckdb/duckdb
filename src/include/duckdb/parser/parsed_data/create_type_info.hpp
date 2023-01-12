//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_type_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

struct CreateTypeInfo : public CreateInfo {
	CreateTypeInfo() : CreateInfo(CatalogType::TYPE_ENTRY) {
	}
	CreateTypeInfo(string name_p, LogicalType type_p)
	    : CreateInfo(CatalogType::TYPE_ENTRY), name(std::move(name_p)), type(std::move(type_p)) {
	}

	//! Name of the Type
	string name;
	//! Logical Type
	LogicalType type;
	//! Used by create enum from query
	unique_ptr<SQLStatement> query;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateTypeInfo>();
		CopyProperties(*result);
		result->name = name;
		result->type = type;
		if (query) {
			result->query = query->Copy();
		}
		return std::move(result);
	}

protected:
	void SerializeInternal(Serializer &) const override {
		throw NotImplementedException("Cannot serialize '%s'", CatalogTypeToString(CreateInfo::type));
	}
};

} // namespace duckdb
