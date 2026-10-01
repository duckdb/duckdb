#include "duckdb/function/function_set.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

ScalarFunctionSet::ScalarFunctionSet() : FunctionSet("") {
}

ScalarFunctionSet::ScalarFunctionSet(Identifier name) : FunctionSet(std::move(name)) {
}

ScalarFunctionSet::ScalarFunctionSet(ScalarFunction fun) : FunctionSet(fun.name) {
	AddFunction(std::move(fun));
}

shared_ptr<const ScalarFunction> ScalarFunctionSet::GetFunctionByArguments(ClientContext &context,
                                                                           const vector<LogicalType> &arguments) {
	ErrorData error;
	FunctionBinder binder(context);
	auto index = binder.BindFunction(name, *this, arguments, error);
	if (!index.IsValid()) {
		throw BinderException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                      error.RawMessage());
	}
	return GetFunctionByOffset(index.GetIndex());
}

AggregateFunctionSet::AggregateFunctionSet() : FunctionSet("") {
}

AggregateFunctionSet::AggregateFunctionSet(Identifier name) : FunctionSet(std::move(name)) {
}

AggregateFunctionSet::AggregateFunctionSet(AggregateFunction fun) : FunctionSet(fun.name) {
	AddFunction(std::move(fun));
}

shared_ptr<const AggregateFunction> AggregateFunctionSet::GetFunctionByArguments(ClientContext &context,
                                                                                 const vector<LogicalType> &arguments) {
	ErrorData error;
	FunctionBinder binder(context);
	auto index = binder.BindFunction(name, *this, arguments, error);
	if (!index.IsValid()) {
		throw BinderException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                      error.RawMessage());
	}
	return GetFunctionByOffset(index.GetIndex());
}

WindowFunctionSet::WindowFunctionSet() : FunctionSet("") {
}

WindowFunctionSet::WindowFunctionSet(Identifier name) : FunctionSet(std::move(name)) {
}

WindowFunctionSet::WindowFunctionSet(WindowFunction fun) : FunctionSet(fun.name) {
	AddFunction(std::move(fun));
}

shared_ptr<const WindowFunction> WindowFunctionSet::GetFunctionByArguments(ClientContext &context,
                                                                           const vector<LogicalType> &arguments) {
	ErrorData error;
	FunctionBinder binder(context);
	auto index = binder.BindFunction(name, *this, arguments, error);
	if (!index.IsValid()) {
		throw BinderException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                      error.RawMessage());
	}
	return GetFunctionByOffset(index.GetIndex());
}

TableFunctionSet::TableFunctionSet(Identifier name) : FunctionSet(std::move(name)) {
}

TableFunctionSet::TableFunctionSet(TableFunction fun) : FunctionSet(fun.name) {
	AddFunction(std::move(fun));
}

shared_ptr<const TableFunction> TableFunctionSet::GetFunctionByArguments(ClientContext &context,
                                                                         const vector<LogicalType> &arguments) {
	ErrorData error;
	FunctionBinder binder(context);
	auto index = binder.BindFunction(name, *this, arguments, error);
	if (!index.IsValid()) {
		throw BinderException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
		                      error.RawMessage());
	}
	return GetFunctionByOffset(index.GetIndex());
}

PragmaFunctionSet::PragmaFunctionSet(Identifier name) : FunctionSet(std::move(name)) {
}

PragmaFunctionSet::PragmaFunctionSet(PragmaFunction fun) : FunctionSet(fun.name) {
	AddFunction(std::move(fun));
}

} // namespace duckdb
