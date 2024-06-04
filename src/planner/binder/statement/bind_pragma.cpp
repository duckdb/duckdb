#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/operator/logical_pragma.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

unique_ptr<BoundPragmaInfo> Binder::BindPragma(PragmaInfo &info, QueryErrorContext error_context) {
	vector<Value> params;
	named_parameter_map_t named_parameters;

	// resolve the parameters
	ConstantBinder pragma_binder(*this, context, "PRAGMA value");
	for (auto &param : info.parameters) {
		auto bound_value = pragma_binder.Bind(param);
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_value, true);
		params.push_back(std::move(value));
	}

	for (auto &entry : info.named_parameters) {
		auto bound_value = pragma_binder.Bind(entry.second);
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_value, true);
		named_parameters.insert(make_pair(entry.first, std::move(value)));
	}

	// bind the pragma function
	auto &entry = Catalog::GetEntry<PragmaFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, info.name);
	FunctionBinder function_binder(context);
	ErrorData error;
	auto bound_idx = function_binder.BindFunction(entry.name, entry.functions, params, error);
	if (!bound_idx.IsValid()) {
		D_ASSERT(error.HasError());
		error.AddQueryLocation(error_context);
		error.Throw();
	}
	auto bound_function = entry.functions.GetFunctionByOffset(bound_idx.GetIndex());
	// bind and check named params
	BindNamedParameters(bound_function.named_parameters, named_parameters, error_context, bound_function.name);
	return make_uniq<BoundPragmaInfo>(std::move(bound_function), std::move(params), std::move(named_parameters));
}

BoundStatement Binder::Bind(PragmaStatement &stmt) {
	// bind the pragma function
	QueryErrorContext error_context(stmt.stmt_location);
	auto bound_info = BindPragma(*stmt.info, error_context);
	if (!bound_info->function.function) {
		throw BinderException("PRAGMA function does not have a function specified");
	}

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalPragma>(std::move(bound_info));

	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
