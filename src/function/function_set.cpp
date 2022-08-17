#include "duckdb/function/function_set.hpp"

namespace duckdb {

ScalarFunctionSet::ScalarFunctionSet(string name) : FunctionSet(move(name)) {
}

ScalarFunction ScalarFunctionSet::GetFunctionByArguments(const vector<LogicalType> &arguments) {
	string error;
	idx_t index = Function::BindFunction(name, *this, arguments, error);
	if (index == DConstants::INVALID_INDEX) {
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error);
	}
	return GetFunctionByOffset(index);
}

AggregateFunctionSet::AggregateFunctionSet(string name) : FunctionSet(move(name)) {
}

AggregateFunction AggregateFunctionSet::GetFunctionByArguments(const vector<LogicalType> &arguments) {
	string error;
	idx_t index = Function::BindFunction(name, *this, arguments, error);
	if (index == DConstants::INVALID_INDEX) {
		// check if the arguments are a prefix of any of the arguments
		// this is used for functions such as quantile or string_agg that delete part of their arguments during bind
		// FIXME: we should come up with a better solution here
		for (auto &func : functions) {
			if (arguments.size() >= func.arguments.size()) {
				continue;
			}
			bool is_prefix = true;
			for (idx_t k = 0; k < arguments.size(); k++) {
				if (arguments[k] != func.arguments[k]) {
					is_prefix = false;
					break;
				}
			}
			if (is_prefix) {
				return func;
			}
		}
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error);
	}
	return GetFunctionByOffset(index);
}

TableFunctionSet::TableFunctionSet(string name) : FunctionSet(move(name)) {
}

TableFunction TableFunctionSet::GetFunctionByArguments(const vector<LogicalType> &arguments) {
	string error;
	idx_t index = Function::BindFunction(name, *this, arguments, error);
	if (index == DConstants::INVALID_INDEX) {
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error);
	}
	return GetFunctionByOffset(index);
}

} // namespace duckdb
