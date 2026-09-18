//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_type_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

#include "duckdb/common/identifier.hpp"
#include "duckdb/function/type_constructor.hpp"

namespace duckdb {

struct CreateTypeInfo : public CreateInfo {
	CreateTypeInfo();
	CreateTypeInfo(string name_p, LogicalType type_p, bind_logical_type_function_t bind_function_p = nullptr);
	CreateTypeInfo(string name_p, LogicalType type_p, TypeConstructorSet constructors_p);

	//! Name of the Type
	const Identifier &GetTypeName() const {
		return qualified_name.Name();
	}
	void SetTypeName(Identifier name) {
		qualified_name = qualified_name.WithName(std::move(name));
	}
	//! Logical Type
	LogicalType type;
	//! Used by create enum from query
	unique_ptr<SQLStatement> query;
	//! The constructors used to bind type modifiers to the type
	TypeConstructorSet constructors;

public:
	unique_ptr<CreateInfo> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;
};

} // namespace duckdb
