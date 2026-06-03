//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {
//! Represents a function call
class FunctionExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::FUNCTION;

public:
	DUCKDB_API FunctionExpression(Identifier catalog_name, Identifier schema_name, const Identifier &function_name,
	                              vector<unique_ptr<ParsedExpression>> children,
	                              unique_ptr<ParsedExpression> filter = nullptr,
	                              unique_ptr<OrderModifier> order_bys = nullptr, bool distinct = false,
	                              bool is_operator = false, bool export_state = false);
	DUCKDB_API FunctionExpression(const Identifier &function_name, vector<unique_ptr<ParsedExpression>> children,
	                              unique_ptr<ParsedExpression> filter = nullptr,
	                              unique_ptr<OrderModifier> order_bys = nullptr, bool distinct = false,
	                              bool is_operator = false, bool export_state = false);

public:
	const string &Catalog() const {
		return catalog.GetName();
	}
	string &CatalogMutable() {
		return catalog.GetNameMutable();
	}
	const string &Schema() const {
		return schema.GetName();
	}
	string &SchemaMutable() {
		return schema.GetNameMutable();
	}
	const string &FunctionName() const {
		return function_name.GetName();
	}
	string &FunctionNameMutable() {
		return function_name.GetNameMutable();
	}
	bool IsOperator() const {
		return is_operator;
	}
	bool &IsOperatorMutable() {
		return is_operator;
	}
	const vector<unique_ptr<ParsedExpression>> &GetChildren() const {
		return children;
	}
	vector<unique_ptr<ParsedExpression>> &GetChildrenMutable() {
		return children;
	}
	bool Distinct() const {
		return distinct;
	}
	bool &DistinctMutable() {
		return distinct;
	}
	const unique_ptr<ParsedExpression> &Filter() const {
		return filter;
	}
	unique_ptr<ParsedExpression> &FilterMutable() {
		return filter;
	}
	const unique_ptr<OrderModifier> &OrderBy() const {
		return order_bys;
	}
	unique_ptr<OrderModifier> &OrderByMutable() {
		return order_bys;
	}
	bool ExportState() const {
		return export_state;
	}
	bool &ExportStateMutable() {
		return export_state;
	}

	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

	void Verify() const override;

	//! Returns a pointer to the lambda expression, if the function has a lambda expression as a child, else nullptr.
	optional_ptr<ParsedExpression> IsLambdaFunction() const;

public:
	template <class T, class BASE, class ORDER_MODIFIER = OrderModifier>
	static string ToString(const T &entry, const string &catalog, const string &schema, const string &function_name,
	                       bool is_operator = false, bool distinct = false, BASE *filter = nullptr,
	                       ORDER_MODIFIER *order_bys = nullptr, bool export_state = false, bool add_alias = false) {
		if (is_operator) {
			// built-in operator
			D_ASSERT(!distinct);
			if (entry.GetChildren().size() == 1) {
				if (StringUtil::Contains(function_name, "__postfix")) {
					return "((" + entry.GetChildren()[0]->ToString() + ")" +
					       StringUtil::Replace(function_name, "__postfix", "") + ")";
				} else {
					return function_name + "(" + entry.GetChildren()[0]->ToString() + ")";
				}
			} else if (entry.GetChildren().size() == 2) {
				return StringUtil::Format("(%s %s %s)", entry.GetChildren()[0]->ToString(), function_name,
				                          entry.GetChildren()[1]->ToString());
			}
		}
		// standard function call
		string result;
		if (!catalog.empty()) {
			result += SQLIdentifier(catalog) + ".";
		}
		if (!schema.empty()) {
			result += SQLIdentifier(schema) + ".";
		}
		result += SQLIdentifier(function_name);
		result += "(";
		if (distinct) {
			result += "DISTINCT ";
		}
		result +=
		    StringUtil::Join(entry.GetChildren(), entry.GetChildren().size(), ", ", [&](const unique_ptr<BASE> &child) {
			    return child->GetAlias().empty() || !add_alias
			               ? child->ToString()
			               : StringUtil::Format("%s := %s", SQLIdentifier(child->GetAlias()), child->ToString());
		    });
		// ordered aggregate
		if (order_bys && !order_bys->orders.empty()) {
			if (entry.GetChildren().empty()) {
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
	//! Catalog of the function
	Identifier catalog;
	//! Schema of the function
	Identifier schema;
	//! Function name
	Identifier function_name;
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

private:
	FunctionExpression();
};
} // namespace duckdb
