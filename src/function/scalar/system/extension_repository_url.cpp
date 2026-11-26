#include "duckdb/function/scalar/system_functions.hpp"

#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_repository_manager.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

//! The connection is captured at bind time: reading what a host publishes is an https request, which takes the
//! settings and secrets of the connection that asks for it
struct ExtensionRepositoryUrlBindData : FunctionData {
	explicit ExtensionRepositoryUrlBindData(ClientContext &context_p) : context(context_p) {
	}

	ClientContext &context;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ExtensionRepositoryUrlBindData>(context);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ExtensionRepositoryUrlBindData>();
		return &context == &other.context;
	}
};

unique_ptr<FunctionData> ExtensionRepositoryUrlBind(BindScalarFunctionInput &input) {
	return make_uniq<ExtensionRepositoryUrlBindData>(input.GetClientContext());
}

void ExtensionRepositoryUrlFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.BindInfo()->Cast<ExtensionRepositoryUrlBindData>();
	auto &context = info.context;
	auto &db = DatabaseInstance::GetDatabase(context);

	// every host costs a request, so a host that repeats within the chunk is resolved once. Nothing is remembered
	// beyond it: the function is volatile, and what a host publishes is its to change
	unordered_map<string, string> resolved;
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t host) {
		auto name = host.GetString();
		auto entry = resolved.find(name);
		if (entry == resolved.end()) {
			auto resolution = ExtensionRepositoryManager::ResolveRepositoryFromHost(
			    db, context, name, ExtensionRepositoryHostCaller::LOOKUP);
			entry = resolved.emplace(std::move(name), std::move(resolution.url)).first;
		}
		return StringVector::AddString(result, entry->second);
	});
}

} // namespace

ScalarFunction ExtensionRepositoryUrl::GetFunction() {
	// the parameter is declared by name: duckdb_functions() lists signatures, and an undeclared one shows as col0
	ScalarFunction function({FunctionParameter(Identifier("host"), LogicalType::VARCHAR)}, LogicalType::VARCHAR,
	                        ExtensionRepositoryUrlFunction, ExtensionRepositoryUrlBind, nullptr, nullptr,
	                        LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
	// a name that is not a host, a host that cannot be reached or a file that is not what it should be are all errors
	// of the call, raised while it runs
	function.SetFallible();
	return function;
}

} // namespace duckdb
