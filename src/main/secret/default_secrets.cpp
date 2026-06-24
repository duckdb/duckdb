#include "duckdb/main/secret/default_secrets.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

vector<SecretType> CreateHTTPSecretFunctions::GetDefaultSecretTypes() {
	vector<SecretType> result;

	// HTTP secret
	SecretType secret_type;
	secret_type.name = "http";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	result.push_back(std::move(secret_type));

	return result;
}

//! Get the default secret functions
vector<CreateSecretFunction> CreateHTTPSecretFunctions::GetDefaultSecretFunctions() {
	vector<CreateSecretFunction> result;

	// HTTP secret CONFIG provider
	CreateSecretFunction http_config_fun;
	http_config_fun.secret_type = "http";
	http_config_fun.provider = "config";
	http_config_fun.function = CreateHTTPSecretFromConfig;

	http_config_fun.named_parameters["verify_ssl"] = LogicalType::BOOLEAN;
	http_config_fun.named_parameters["http_proxy"] = LogicalType::VARCHAR;
	http_config_fun.named_parameters["http_proxy_password"] = LogicalType::VARCHAR;
	http_config_fun.named_parameters["http_proxy_username"] = LogicalType::VARCHAR;

	http_config_fun.named_parameters["extra_http_headers"] =
	    LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	http_config_fun.named_parameters["bearer_token"] = LogicalType::VARCHAR;
	result.push_back(std::move(http_config_fun));

	// HTTP secret ENV provider
	CreateSecretFunction http_env_fun;
	http_env_fun.secret_type = "http";
	http_env_fun.provider = "env";
	http_env_fun.function = CreateHTTPSecretFromEnv;

	http_env_fun.named_parameters["verify_ssl"] = LogicalType::BOOLEAN;
	http_env_fun.named_parameters["http_proxy"] = LogicalType::VARCHAR;
	http_env_fun.named_parameters["http_proxy_password"] = LogicalType::VARCHAR;
	http_env_fun.named_parameters["http_proxy_username"] = LogicalType::VARCHAR;

	http_env_fun.named_parameters["extra_http_headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	http_env_fun.named_parameters["bearer_token"] = LogicalType::VARCHAR;
	result.push_back(std::move(http_env_fun));

	return result;
}

static const char *TryGetEnv(const char *name) {
	const char *res = std::getenv(name);
	if (res) {
		return res;
	}
	return std::getenv(StringUtil::Upper(name).c_str());
}

unique_ptr<BaseSecret> CreateHTTPSecretFunctions::CreateHTTPSecretFromEnv(ClientContext &context,
                                                                          CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	auto http_proxy = TryGetEnv("http_proxy");
	if (http_proxy) {
		secret->secret_map["http_proxy"] = Value(http_proxy);
	}
	auto http_proxy_password = TryGetEnv("http_proxy_password");
	if (http_proxy_password) {
		secret->secret_map["http_proxy_password"] = Value(http_proxy_password);
	}
	auto http_proxy_username = TryGetEnv("http_proxy_username");
	if (http_proxy_username) {
		secret->secret_map["http_proxy_username"] = Value(http_proxy_username);
	}

	// Allow overwrites
	secret->TrySetValue("verify_ssl", input);
	secret->TrySetValue("http_proxy", input);
	secret->TrySetValue("http_proxy_password", input);
	secret->TrySetValue("http_proxy_username", input);

	secret->TrySetValue("extra_http_headers", input);
	secret->TrySetValue("bearer_token", input);

	//! Set redact keys
	secret->redact_keys = {"http_proxy_password", "bearer_token"};

	return std::move(secret);
}

unique_ptr<BaseSecret> CreateHTTPSecretFunctions::CreateHTTPSecretFromConfig(ClientContext &context,
                                                                             CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	secret->TrySetValue("verify_ssl", input);
	secret->TrySetValue("http_proxy", input);
	secret->TrySetValue("http_proxy_password", input);
	secret->TrySetValue("http_proxy_username", input);

	secret->TrySetValue("extra_http_headers", input);
	secret->TrySetValue("bearer_token", input);

	//! Set redact keys
	secret->redact_keys = {"http_proxy_password", "bearer_token"};

	return std::move(secret);
}

vector<SecretType> CreateExtensionRepositorySecretFunctions::GetDefaultSecretTypes() {
	vector<SecretType> result;

	// extension_repository secret: pins a self-hosted repository's public signing key
	SecretType secret_type;
	secret_type.name = "extension_repository";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	result.push_back(std::move(secret_type));

	return result;
}

vector<CreateSecretFunction> CreateExtensionRepositorySecretFunctions::GetDefaultSecretFunctions() {
	vector<CreateSecretFunction> result;

	CreateSecretFunction fun;
	fun.secret_type = "extension_repository";
	fun.provider = "config";
	fun.function = CreateExtensionRepositorySecretFromConfig;

	fun.named_parameters["url"] = LogicalType::VARCHAR;
	fun.named_parameters["signing_key"] = LogicalType::VARCHAR;
	fun.named_parameters["key_id"] = LogicalType::VARCHAR;
	fun.named_parameters["valid_to"] = LogicalType::VARCHAR;
	fun.named_parameters["discovery_url"] = LogicalType::VARCHAR;
	fun.named_parameters["url_template"] = LogicalType::VARCHAR;
	result.push_back(std::move(fun));

	return result;
}

unique_ptr<BaseSecret>
CreateExtensionRepositorySecretFunctions::CreateExtensionRepositorySecretFromConfig(ClientContext &context,
                                                                                    CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		// Default the scope to the repository url, so a load-time LookupSecret(url) matches by prefix
		auto url_entry = input.options.find("url");
		if (url_entry != input.options.end()) {
			scope.push_back(url_entry->second.ToString());
		} else {
			scope.push_back("");
		}
	}

	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
	secret->TrySetValue("url", input);
	secret->TrySetValue("signing_key", input);
	secret->TrySetValue("key_id", input);
	secret->TrySetValue("valid_to", input);
	secret->TrySetValue("discovery_url", input);
	secret->TrySetValue("url_template", input);

	// The signing key is a public key - nothing to redact here.
	return std::move(secret);
}

} // namespace duckdb
