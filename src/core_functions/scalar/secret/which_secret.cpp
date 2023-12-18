#include "duckdb/core_functions/scalar/secret_functions.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

static void WhichSecretFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto &secret_manager = SecretManager::Get(state.GetContext());
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(state.GetContext());

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t path, string_t type) {
		    auto secret_entry = secret_manager.GetSecretByPath(transaction, path.GetString(), type.GetString());
		    if (!secret_entry) {
			    return string_t();
		    }
		    return StringVector::AddString(result, secret_entry->name);
	    });
}

ScalarFunction WhichSecretFun::GetFunction() {
	ScalarFunction which_secret("which_secret", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                            WhichSecretFunction, nullptr, nullptr, nullptr, nullptr);
	return which_secret;
}

} // namespace duckdb
