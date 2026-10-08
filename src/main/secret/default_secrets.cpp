#include "duckdb/main/secret/default_secrets.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/os_util.hpp"

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

static bool TryGetEnv(ClientContext &context, const string &name, string &value) {
	auto &os_util = OSUtil::Get(*context.db);
	return os_util.TryGetEnv(name, value) || os_util.TryGetEnv(StringUtil::Upper(name), value);
}

unique_ptr<BaseSecret> CreateHTTPSecretFunctions::CreateHTTPSecretFromEnv(ClientContext &context,
                                                                          CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	string value;
	if (TryGetEnv(context, "http_proxy", value)) {
		secret->secret_map["http_proxy"] = Value(value);
	}
	if (TryGetEnv(context, "http_proxy_password", value)) {
		secret->secret_map["http_proxy_password"] = Value(value);
	}
	if (TryGetEnv(context, "http_proxy_username", value)) {
		secret->secret_map["http_proxy_username"] = Value(value);
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

} // namespace duckdb
