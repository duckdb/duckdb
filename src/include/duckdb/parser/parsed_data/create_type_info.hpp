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

	//! Name of the Type
	string name;
	//! Logical Type
	LogicalType type;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateTypeInfo>();
		CopyProperties(*result);
		result->name = name;
		result->type = type;
		return move(result);
	}
};

} // namespace duckdb
