#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_repository_discovery.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

struct RegisterExtensionRepoBindData : public FunctionData {
	RegisterExtensionRepoBindData(string repo_name_p, string repo_url_p)
	    : repo_name(std::move(repo_name_p)), repo_url(std::move(repo_url_p)) {
	}
	string repo_name;
	string repo_url;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RegisterExtensionRepoBindData>(repo_name, repo_url);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RegisterExtensionRepoBindData>();
		return repo_name == other.repo_name && repo_url == other.repo_url;
	}
};

struct RegisterExtensionRepoState : public GlobalTableFunctionState {
	RegisterExtensionRepoState() : executed(false) {
	}
	bool executed;
};

static unique_ptr<FunctionData> RegisterExtensionRepoBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw InvalidInputException("duckdb_register_extension_repo: name and url must not be NULL");
	}
	auto repo_name = input.inputs[0].GetValue<string>();
	auto repo_url = input.inputs[1].GetValue<string>();

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

	return make_uniq<RegisterExtensionRepoBindData>(std::move(repo_name), std::move(repo_url));
}

static unique_ptr<GlobalTableFunctionState> RegisterExtensionRepoInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<RegisterExtensionRepoState>();
}

static void RegisterExtensionRepoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RegisterExtensionRepoState>();
	if (state.executed) {
		return;
	}
	state.executed = true;
	auto &bind_data = data_p.bind_data->Cast<RegisterExtensionRepoBindData>();

	if (!Settings::Get<AllowCustomExtensionRepositoriesSetting>(context)) {
		throw InvalidInputException(
		    "duckdb_register_extension_repo requires SET allow_custom_extension_repositories=true");
	}

	// 1. Resolve and fetch the discovery document
	auto discovery_url = ExtensionRepositoryDiscovery::NormalizeDiscoveryUrl(bind_data.repo_url);
	auto discovery = ExtensionRepositoryDiscovery::FetchAndParse(context, discovery_url);

	// 2. Determine the canonical repository base url
	string base_url;
	if (!discovery.url.empty()) {
		base_url = discovery.url;
	} else if (StringUtil::EndsWith(StringUtil::Lower(bind_data.repo_url), ".json")) {
		throw InvalidInputException("Discovery document '%s' must advertise a 'url' (repository base) when registering "
		                            "via a discovery (.json) url",
		                            discovery_url);
	} else {
		base_url = bind_data.repo_url;
	}

	// 3. Pin the first advertised signing key (MVP: single key)
	auto &primary_key = discovery.signing_keys[0];

	CreateSecretInput secret_input;
	secret_input.type = "extension_repository";
	secret_input.provider = "config";
	secret_input.name = Identifier(bind_data.repo_name);
	secret_input.scope = {base_url};
	secret_input.options["url"] = Value(base_url);
	secret_input.options["signing_key"] = Value(primary_key.public_key);
	if (!primary_key.kid.empty()) {
		secret_input.options["key_id"] = Value(primary_key.kid);
	}
	if (!primary_key.valid_to.empty()) {
		secret_input.options["valid_to"] = Value(primary_key.valid_to);
	}
	secret_input.options["discovery_url"] = Value(discovery_url);
	if (!discovery.url_template.empty()) {
		secret_input.options["url_template"] = Value(discovery.url_template);
	}
	secret_input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	secret_input.persist_type = SecretPersistType::DEFAULT;

	auto &secret_manager = SecretManager::Get(context);
	secret_manager.CreateSecret(context, secret_input);

	// 4. Report the pinned key(s) (the explicit TOFU moment)
	for (auto &sk : discovery.signing_keys) {
		output.data[0].Append(Value(bind_data.repo_name));
		output.data[1].Append(Value(base_url));
		output.data[2].Append(sk.kid.empty() ? Value(LogicalType::VARCHAR) : Value(sk.kid));
		output.data[3].Append(Value(ExtensionRepositoryDiscovery::KeyFingerprint(sk.public_key)));
		output.data[4].Append(sk.valid_to.empty() ? Value(LogicalType::VARCHAR) : Value(sk.valid_to));
	}
}

void DuckDBRegisterExtensionRepoFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_register_extension_repo", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                              RegisterExtensionRepoFunction, RegisterExtensionRepoBind, RegisterExtensionRepoInit));
}

} // namespace duckdb
