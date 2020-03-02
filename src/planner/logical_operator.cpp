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

vector<ColumnBinding> LogicalOperator::GenerateColumnBindings(idx_t table_idx, idx_t column_count) {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(ColumnBinding(table_idx, i));
	}
	return result;
}

vector<TypeId> LogicalOperator::MapTypes(vector<TypeId> types, vector<idx_t> projection_map) {
	if (projection_map.size() == 0) {
		return types;
	} else {
		vector<TypeId> result_types;
		for (auto index : projection_map) {
			result_types.push_back(types[index]);
		}
		return result_types;
	}
}

vector<ColumnBinding> LogicalOperator::MapBindings(vector<ColumnBinding> bindings, vector<idx_t> projection_map) {
	if (projection_map.size() == 0) {
		return bindings;
	} else {
		vector<ColumnBinding> result_bindings;
		for (auto index : projection_map) {
			result_bindings.push_back(bindings[index]);
		}
		return result_bindings;
	}
}

string LogicalOperator::ToString(idx_t depth) const {
	string result = LogicalOperatorToString(type);
	result += ParamsToString();
	if (children.size() > 0) {
		for (idx_t i = 0; i < children.size(); i++) {
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
