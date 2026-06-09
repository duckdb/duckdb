//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/type_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

class TypeExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::TYPE;

	TypeExpression(string catalog, string schema, string type_name, vector<unique_ptr<ParsedExpression>> children);
	TypeExpression(string type_name, vector<unique_ptr<ParsedExpression>> children);

public:
	const string &GetTypeName() const {
		return type_name;
	}
	const string &GetSchema() const {
		return schema;
	}
	const string &GetCatalog() const {
		return catalog;
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

	static bool Equal(const TypeExpression &a, const TypeExpression &b);
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

	void Verify() const override;

private:
	TypeExpression();

	//! Qualified name parts
	string catalog;
	string schema;
	string type_name;

	//! Children of the type expression (e.g. type parameters)
	vector<unique_ptr<ParsedExpression>> children;
};

} // namespace duckdb
