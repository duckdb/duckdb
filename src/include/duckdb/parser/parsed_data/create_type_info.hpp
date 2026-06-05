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

namespace duckdb {

class TypeArgument {
public:
	TypeArgument(string name_p, Value value_p) : name(std::move(name_p)), value(std::move(value_p)) {
	}
	const string &GetName() const {
		return name;
	}
	const Value &GetValue() const {
		return value;
	}
	bool HasName() const {
		return !name.empty();
	}
	bool IsNamed(const char *name_to_check) const {
		return StringUtil::CIEquals(name, name_to_check);
	}
	bool IsNotNull() const {
		return !value.IsNull();
	}
	const LogicalType &GetType() const {
		return value.type();
	}

private:
	string name;
	Value value;
};

struct BindLogicalTypeInput {
	optional_ptr<ClientContext> context;
	const LogicalType &base_type;
	const vector<TypeArgument> &modifiers;
};

//! The type to bind type modifiers to a type
typedef LogicalType (*bind_logical_type_function_t)(BindLogicalTypeInput &input);

struct CreateTypeInfo : public CreateInfo {
	CreateTypeInfo();
	CreateTypeInfo(string name_p, LogicalType type_p, bind_logical_type_function_t bind_function_p = nullptr);

	//! Name of the Type
	string name;
	//! Logical Type
	LogicalType type;
	//! Used by create enum from query
	unique_ptr<SQLStatement> query;
	//! Bind type modifiers to the type
	bind_logical_type_function_t bind_function;

public:
	unique_ptr<CreateInfo> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;
};

} // namespace duckdb
