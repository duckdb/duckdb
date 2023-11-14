#include "duckdb/function/function_set.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

ScalarFunctionSet::ScalarFunctionSet() : FunctionSet("") {
}

ScalarFunctionSet::ScalarFunctionSet(string name) : FunctionSet(std::move(name)) {
}

ScalarFunctionSet::ScalarFunctionSet(ScalarFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

ScalarFunction ScalarFunctionSet::GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments) {
	string error;
	FunctionBinder binder(context);
	idx_t index = binder.BindFunction(name, *this, arguments, error);
	if (index == DConstants::INVALID_INDEX) {
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error);
	}
	return GetFunctionByOffset(index);
}

AggregateFunctionSet::AggregateFunctionSet() : FunctionSet("") {
}

AggregateFunctionSet::AggregateFunctionSet(string name) : FunctionSet(std::move(name)) {
}

AggregateFunctionSet::AggregateFunctionSet(AggregateFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

AggregateFunction AggregateFunctionSet::GetFunctionByArguments(ClientContext &context,
                                                               const vector<LogicalType> &arguments) {
	string error;
	FunctionBinder binder(context);
	idx_t index = binder.BindFunction(name, *this, arguments, error);
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

TableFunctionSet::TableFunctionSet(string name) : FunctionSet(std::move(name)) {
}

TableFunctionSet::TableFunctionSet(TableFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

TableFunction TableFunctionSet::GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments) {
	string error;
	FunctionBinder binder(context);
	idx_t index = binder.BindFunction(name, *this, arguments, error);
	if (index == DConstants::INVALID_INDEX) {
		throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                        error);
	}
	return GetFunctionByOffset(index);
}

PragmaFunctionSet::PragmaFunctionSet(string name) : FunctionSet(std::move(name)) {
}

PragmaFunctionSet::PragmaFunctionSet(PragmaFunction fun) : FunctionSet(std::move(fun.name)) {
	functions.push_back(std::move(fun));
}

} // namespace duckdb
