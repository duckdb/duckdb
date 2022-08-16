//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/operator_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {
//! Represents a built-in operator expression
class OperatorExpression : public ParsedExpression {
public:
	DUCKDB_API explicit OperatorExpression(ExpressionType type, unique_ptr<ParsedExpression> left = nullptr,
	                                       unique_ptr<ParsedExpression> right = nullptr);
	DUCKDB_API OperatorExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children);

	vector<unique_ptr<ParsedExpression>> children;

public:
	string ToString() const override;

	static bool Equals(const OperatorExpression *a, const OperatorExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		auto op = ExpressionTypeToOperator(entry.type);
		if (!op.empty()) {
			// use the operator string to represent the operator
			D_ASSERT(entry.children.size() == 2);
			return entry.children[0]->ToString() + " " + op + " " + entry.children[1]->ToString();
		}
		switch (entry.type) {
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN: {
			string op_type = entry.type == ExpressionType::COMPARE_IN ? " IN " : " NOT IN ";
			string in_child = entry.children[0]->ToString();
			string child_list = "(";
			for (idx_t i = 1; i < entry.children.size(); i++) {
				if (i > 1) {
					child_list += ", ";
				}
				child_list += entry.children[i]->ToString();
			}
			child_list += ")";
			return "(" + in_child + op_type + child_list + ")";
		}
		case ExpressionType::OPERATOR_NOT: {
			string result = "(";
			result += ExpressionTypeToString(entry.type);
			result += " ";
			result += StringUtil::Join(entry.children, entry.children.size(), ", ",
			                           [](const unique_ptr<BASE> &child) { return child->ToString(); });
			result += ")";
			return result;
		}
		case ExpressionType::GROUPING_FUNCTION:
		case ExpressionType::OPERATOR_COALESCE: {
			string result = ExpressionTypeToString(entry.type);
			result += "(";
			result += StringUtil::Join(entry.children, entry.children.size(), ", ",
			                           [](const unique_ptr<BASE> &child) { return child->ToString(); });
			result += ")";
			return result;
		}
		case ExpressionType::OPERATOR_IS_NULL:
			return "(" + entry.children[0]->ToString() + " IS NULL)";
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			return "(" + entry.children[0]->ToString() + " IS NOT NULL)";
		case ExpressionType::ARRAY_EXTRACT:
			return entry.children[0]->ToString() + "[" + entry.children[1]->ToString() + "]";
		case ExpressionType::ARRAY_SLICE:
			return entry.children[0]->ToString() + "[" + entry.children[1]->ToString() + ":" +
			       entry.children[2]->ToString() + "]";
		case ExpressionType::STRUCT_EXTRACT: {
			if (entry.children[1]->type != ExpressionType::VALUE_CONSTANT) {
				return string();
			}
			auto child_string = entry.children[1]->ToString();
			D_ASSERT(child_string.size() >= 3);
			D_ASSERT(child_string[0] == '\'' && child_string[child_string.size() - 1] == '\'');
			return "(" + entry.children[0]->ToString() + ")." +
			       KeywordHelper::WriteOptionallyQuoted(child_string.substr(1, child_string.size() - 2));
		}
		case ExpressionType::ARRAY_CONSTRUCTOR: {
			string result = "(ARRAY[";
			result += StringUtil::Join(entry.children, entry.children.size(), ", ",
			                           [](const unique_ptr<BASE> &child) { return child->ToString(); });
			result += "])";
			return result;
		}
		default:
			throw InternalException("Unrecognized operator type");
		}
	}
};

} // namespace duckdb
