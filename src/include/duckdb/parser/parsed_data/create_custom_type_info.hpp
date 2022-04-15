//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_custom_type_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {

struct CreateCustomTypeInfo : public CreateInfo {

	CreateCustomTypeInfo() : CreateInfo(CatalogType::TYPE_CUSTOM_ENTRY) {
	}

	//! Name of the Type
	string name;
	//! Logical Type
	LogicalType type;

	// map<CustomTypeParameterId, string> parameters;
public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateCustomTypeInfo>();
		CopyProperties(*result);
		result->name = name;
		result->type = type;
		// for (auto &iter : parameters) {
		// 	result->parameters.insert(iter);
		// }
		return move(result);
	}
};

} // namespace duckdb