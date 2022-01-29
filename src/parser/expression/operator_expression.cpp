#include "duckdb/parser/expression/operator_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

OperatorExpression::OperatorExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                       unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::OPERATOR) {
	if (left) {
		children.push_back(move(left));
	}
	if (right) {
		children.push_back(move(right));
	}
}

OperatorExpression::OperatorExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children)
    : ParsedExpression(type, ExpressionClass::OPERATOR), children(move(children)) {
}

string OperatorExpression::ToString() const {
	auto op = ExpressionTypeToOperator(type);
	if (!op.empty()) {
		// use the operator string to represent the operator
		D_ASSERT(children.size() == 2);
		return children[0]->ToString() + " " + op + " " + children[1]->ToString();
	}
	switch (type) {
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN: {
		string op_type = type == ExpressionType::COMPARE_IN ? " IN " : " NOT IN ";
		string in_child = children[0]->ToString();
		string child_list = "(";
		for (idx_t i = 1; i < children.size(); i++) {
			if (i > 1) {
				child_list += ", ";
			}
			child_list += children[i]->ToString();
		}
		child_list += ")";
		return "(" + in_child + op_type + child_list + ")";
	}
	case ExpressionType::OPERATOR_NOT:
	case ExpressionType::GROUPING_FUNCTION:
	case ExpressionType::OPERATOR_COALESCE: {
		string result = ExpressionTypeToString(type);
		result += "(";
		result += StringUtil::Join(children, children.size(), ", ",
		                           [](const unique_ptr<ParsedExpression> &child) { return child->ToString(); });
		result += ")";
		return result;
	}
	case ExpressionType::OPERATOR_IS_NULL:
		return "(" + children[0]->ToString() + " IS NULL)";
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return "(" + children[0]->ToString() + " IS NOT NULL)";
	case ExpressionType::ARRAY_EXTRACT:
		return children[0]->ToString() + "[" + children[1]->ToString() + "]";
	case ExpressionType::ARRAY_SLICE:
		return children[0]->ToString() + "[" + children[1]->ToString() + ":" + children[2]->ToString() + "]";
	case ExpressionType::STRUCT_EXTRACT: {
		D_ASSERT(children[1]->type == ExpressionType::VALUE_CONSTANT);
		auto child_string = children[1]->ToString();
		D_ASSERT(child_string.size() >= 3);
		D_ASSERT(child_string[0] == '\'' && child_string[child_string.size() - 1] == '\'');
		return "(" + children[0]->ToString() + ")." +
		       QualifiedName::Quote(child_string.substr(1, child_string.size() - 2));
	}
	case ExpressionType::ARRAY_CONSTRUCTOR: {
		string result = "ARRAY[";
		result += StringUtil::Join(children, children.size(), ", ",
		                           [](const unique_ptr<ParsedExpression> &child) { return child->ToString(); });
		result += "]";
		return result;
	}
	default:
		throw InternalException("Unrecognized operator type");
	}
}

bool OperatorExpression::Equals(const OperatorExpression *a, const OperatorExpression *b) {
	if (a->children.size() != b->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->children.size(); i++) {
		if (!a->children[i]->Equals(b->children[i].get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<ParsedExpression> OperatorExpression::Copy() const {
	auto copy = make_unique<OperatorExpression>(type);
	copy->CopyProperties(*this);
	for (auto &it : children) {
		copy->children.push_back(it->Copy());
	}
	return move(copy);
}

void OperatorExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(children);
}

unique_ptr<ParsedExpression> OperatorExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto expression = make_unique<OperatorExpression>(type);
	expression->children = reader.ReadRequiredSerializableList<ParsedExpression>();
	return move(expression);
}

} // namespace duckdb
