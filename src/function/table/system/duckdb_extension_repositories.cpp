#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/identifier.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/extension/extension_repository_discovery.hpp"

namespace duckdb {

struct DuckDBExtensionRepositoriesData : public GlobalTableFunctionState {
	DuckDBExtensionRepositoriesData() : offset(0) {
	}
	idx_t offset;
	duckdb::vector<SecretEntry> secrets;
};

static unique_ptr<FunctionData> DuckDBExtensionRepositoriesBind(ClientContext &context, TableFunctionBindInput &input,
                                                                vector<LogicalType> &return_types,
                                                                vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("url");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("key_id");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("key_fingerprint");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("valid_to");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBExtensionRepositoriesInit(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
	return make_uniq<DuckDBExtensionRepositoriesData>();
}

static Value GetSecretVarchar(const KeyValueSecret &secret, const string &key) {
	Value result;
	if (secret.TryGetValue(Identifier(key), result) && !result.IsNull()) {
		return Value(result.ToString());
	}
	return Value(LogicalType::VARCHAR);
}

static void DuckDBExtensionRepositoriesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBExtensionRepositoriesData>();

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (data.secrets.empty()) {
		data.secrets = secret_manager.AllSecrets(transaction);
	}

	idx_t count = 0;
	while (data.offset < data.secrets.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.secrets[data.offset];
		data.offset++;
		if (!entry.secret || entry.secret->GetType() != "extension_repository") {
			continue;
		}
		auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*entry.secret);

		auto signing_key = GetSecretVarchar(kv_secret, "signing_key");
		Value fingerprint(LogicalType::VARCHAR);
		if (!signing_key.IsNull()) {
			fingerprint = Value(ExtensionRepositoryDiscovery::KeyFingerprint(signing_key.ToString()));
		}

		output.data[0].Append(Value(entry.secret->GetName()));
		output.data[1].Append(GetSecretVarchar(kv_secret, "url"));
		output.data[2].Append(GetSecretVarchar(kv_secret, "key_id"));
		output.data[3].Append(fingerprint);
		output.data[4].Append(GetSecretVarchar(kv_secret, "valid_to"));
		count++;
	}
}

void DuckDBExtensionRepositoriesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_extension_repositories", {}, DuckDBExtensionRepositoriesFunction,
	                              DuckDBExtensionRepositoriesBind, DuckDBExtensionRepositoriesInit));
}

} // namespace duckdb
