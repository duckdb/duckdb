//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/type_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

class TypeExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::TYPE;

	TypeExpression(Identifier catalog, Identifier schema, Identifier type_name,
	               vector<unique_ptr<ParsedExpression>> children);
	TypeExpression(Identifier type_name, vector<unique_ptr<ParsedExpression>> children);
	TypeExpression(const string &type_name, vector<unique_ptr<ParsedExpression>> children);

public:
	const Identifier &GetTypeName() const {
		return type_name;
	}
	const Identifier &GetSchema() const {
		return schema;
	}
	void SetSchema(Identifier new_schema) {
		schema = std::move(new_schema);
	}
	const Identifier &GetCatalog() const {
		return catalog;
	}
	void SetCatalog(Identifier new_catalog) {
		catalog = std::move(new_catalog);
	}
	const vector<unique_ptr<ParsedExpression>> &GetChildren() const {
		return children;
	}
	vector<unique_ptr<ParsedExpression>> &GetChildren() {
		return children;
	}

public:
	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

	void Verify() const override;

private:
	TypeExpression();

	//! Qualified name parts
	Identifier catalog;
	Identifier schema;
	Identifier type_name;

	//! Children of the type expression (e.g. type parameters)
	vector<unique_ptr<ParsedExpression>> children;
};

} // namespace duckdb
