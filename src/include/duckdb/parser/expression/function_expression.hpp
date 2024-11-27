//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {
//! Represents a function call
class FunctionExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::FUNCTION;

public:
	DUCKDB_API FunctionExpression(string catalog_name, string schema_name, const string &function_name,
	                              vector<unique_ptr<ParsedExpression>> children,
	                              unique_ptr<ParsedExpression> filter = nullptr,
	                              unique_ptr<OrderModifier> order_bys = nullptr, bool distinct = false,
	                              bool is_operator = false, bool export_state = false);
	DUCKDB_API FunctionExpression(const string &function_name, vector<unique_ptr<ParsedExpression>> children,
	                              unique_ptr<ParsedExpression> filter = nullptr,
	                              unique_ptr<OrderModifier> order_bys = nullptr, bool distinct = false,
	                              bool is_operator = false, bool export_state = false);

	//! Catalog of the function
	string catalog;
	//! Schema of the function
	string schema;
	//! Function name
	string function_name;
	//! Whether or not the function is an operator, only used for rendering
	bool is_operator;
	//! List of arguments to the function
	vector<unique_ptr<ParsedExpression>> children;
	//! Whether or not the aggregate function is distinct, only used for aggregates
	bool distinct;
	//! Expression representing a filter, only used for aggregates
	unique_ptr<ParsedExpression> filter;
	//! Modifier representing an ORDER BY, only used for aggregates
	unique_ptr<OrderModifier> order_bys;
	//! whether this function should export its state or not
	bool export_state;

public:
	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	static bool Equal(const FunctionExpression &a, const FunctionExpression &b);
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

	void Verify() const override;

	//! Returns true, if the function has a lambda expression as a child.
	bool IsLambdaFunction() const;

public:
	template <class T, class BASE, class ORDER_MODIFIER = OrderModifier>
	static string ToString(const T &entry, const string &catalog, const string &schema, const string &function_name,
	                       bool is_operator = false, bool distinct = false, BASE *filter = nullptr,
	                       ORDER_MODIFIER *order_bys = nullptr, bool export_state = false, bool add_alias = false) {
		if (is_operator) {
			// built-in operator
			D_ASSERT(!distinct);
			if (entry.children.size() == 1) {
				if (StringUtil::Contains(function_name, "__postfix")) {
					return "((" + entry.children[0]->ToString() + ")" +
					       StringUtil::Replace(function_name, "__postfix", "") + ")";
				} else {
					return function_name + "(" + entry.children[0]->ToString() + ")";
				}
			} else if (entry.children.size() == 2) {
				return StringUtil::Format("(%s %s %s)", entry.children[0]->ToString(), function_name,
				                          entry.children[1]->ToString());
			}
		}
		// standard function call
		string result;
		if (!catalog.empty()) {
			result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
		}
		if (!schema.empty()) {
			result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
		}
		result += KeywordHelper::WriteOptionallyQuoted(function_name);
		result += "(";
		if (distinct) {
			result += "DISTINCT ";
		}
		result += StringUtil::Join(entry.children, entry.children.size(), ", ", [&](const unique_ptr<BASE> &child) {
			return child->alias.empty() || !add_alias
			           ? child->ToString()
			           : StringUtil::Format("%s := %s", SQLIdentifier(child->alias), child->ToString());
		});
		// ordered aggregate
		if (order_bys && !order_bys->orders.empty()) {
			if (entry.children.empty()) {
				result += ") WITHIN GROUP (";
			}
			result += " ORDER BY ";
			for (idx_t i = 0; i < order_bys->orders.size(); i++) {
				if (i > 0) {
					result += ", ";
				}
				result += order_bys->orders[i].ToString();
			}
		}
		result += ")";

		// filtered aggregate
		if (filter) {
			result += " FILTER (WHERE " + filter->ToString() + ")";
		}

		if (export_state) {
			result += " EXPORT_STATE";
		}

		return result;
	}

private:
	FunctionExpression();
};
} // namespace duckdb
