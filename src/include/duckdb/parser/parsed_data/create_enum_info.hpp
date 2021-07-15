//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_enum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

struct CreateEnumInfo : public CreateInfo {

	CreateEnumInfo() : CreateInfo(CatalogType::ENUM_ENTRY) {
	}

	//! Name of the Enum
	string name;
	//! List of Enum values
	vector<string> values;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateEnumInfo>();
		CopyProperties(*result);
		result->name = name;
		result->values = values;
		return move(result);
	}
};

} // namespace duckdb
