#include "duckdb/planner/logical_operator.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

string LogicalOperator::ParamsToString() const {
	string result = "";
	if (expressions.size() > 0) {
		result += "[";
		result += StringUtil::Join(expressions, expressions.size(), ", ",
		                           [](const unique_ptr<Expression> &expression) { return expression->GetName(); });
		result += "]";
	}

	return result;
}

void LogicalOperator::ResolveOperatorTypes() {
	// if (types.size() > 0) {
	// 	// types already resolved for this node
	// 	return;
	// }
	types.clear();
	// first resolve child types
	for (auto &child : children) {
		child->ResolveOperatorTypes();
	}
	// now resolve the types for this operator
	ResolveTypes();
}

string LogicalOperator::ToString(index_t depth) const {
	string result = LogicalOperatorToString(type);
	result += ParamsToString();
	if (children.size() > 0) {
		for (index_t i = 0; i < children.size(); i++) {
			result += "\n" + string(depth * 4, ' ');
			auto &child = children[i];
			result += child->ToString(depth + 1);
		}
		result += "";
	}
	return result;
}

void LogicalOperator::Print() {
	Printer::Print(ToString());
}
