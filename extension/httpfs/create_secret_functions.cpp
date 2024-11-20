#include "create_secret_functions.hpp"
#include "s3fs.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

void CreateS3SecretFunctions::Register(DatabaseInstance &instance) {
	RegisterCreateSecretFunction(instance, "s3");
	RegisterCreateSecretFunction(instance, "r2");
	RegisterCreateSecretFunction(instance, "gcs");
}

unique_ptr<BaseSecret> CreateS3SecretFunctions::CreateSecretFunctionInternal(ClientContext &context,
                                                                             CreateSecretInput &input) {
	// Set scope to user provided scope or the default
	auto scope = input.scope;
	if (scope.empty()) {
		if (input.type == "s3") {
			scope.push_back("s3://");
			scope.push_back("s3n://");
			scope.push_back("s3a://");
		} else if (input.type == "r2") {
			scope.push_back("r2://");
		} else if (input.type == "gcs") {
			scope.push_back("gcs://");
			scope.push_back("gs://");
		} else {
			throw InternalException("Unknown secret type found in httpfs extension: '%s'", input.type);
		}
	}

	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
	secret->redact_keys = {"secret", "session_token"};

	// for r2 we can set the endpoint using the account id
	if (input.type == "r2" && input.options.find("account_id") != input.options.end()) {
		secret->secret_map["endpoint"] = input.options["account_id"].ToString() + ".r2.cloudflarestorage.com";
		secret->secret_map["url_style"] = "path";
	}

	// apply any overridden settings
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "key_id") {
			secret->secret_map["key_id"] = named_param.second;
		} else if (lower_name == "secret") {
			secret->secret_map["secret"] = named_param.second;
		} else if (lower_name == "region") {
			secret->secret_map["region"] = named_param.second.ToString();
		} else if (lower_name == "session_token") {
			secret->secret_map["session_token"] = named_param.second.ToString();
		} else if (lower_name == "endpoint") {
			secret->secret_map["endpoint"] = named_param.second.ToString();
		} else if (lower_name == "url_style") {
			secret->secret_map["url_style"] = named_param.second.ToString();
		} else if (lower_name == "use_ssl") {
			if (named_param.second.type() != LogicalType::BOOLEAN) {
				throw InvalidInputException("Invalid type past to secret option: '%s', found '%s', expected: 'BOOLEAN'",
				                            lower_name, named_param.second.type().ToString());
			}
			secret->secret_map["use_ssl"] = Value::BOOLEAN(named_param.second.GetValue<bool>());
		} else if (lower_name == "url_compatibility_mode") {
			if (named_param.second.type() != LogicalType::BOOLEAN) {
				throw InvalidInputException("Invalid type past to secret option: '%s', found '%s', expected: 'BOOLEAN'",
				                            lower_name, named_param.second.type().ToString());
			}
			secret->secret_map["url_compatibility_mode"] = Value::BOOLEAN(named_param.second.GetValue<bool>());
		} else if (lower_name == "account_id") {
			continue; // handled already
		} else {
			throw InternalException("Unknown named parameter passed to CreateSecretFunctionInternal: " + lower_name);
		}
	}

	return std::move(secret);
}

unique_ptr<BaseSecret> CreateS3SecretFunctions::CreateS3SecretFromConfig(ClientContext &context,
                                                                         CreateSecretInput &input) {
	return CreateSecretFunctionInternal(context, input);
}

void CreateS3SecretFunctions::SetBaseNamedParams(CreateSecretFunction &function, string &type) {
	function.named_parameters["key_id"] = LogicalType::VARCHAR;
	function.named_parameters["secret"] = LogicalType::VARCHAR;
	function.named_parameters["region"] = LogicalType::VARCHAR;
	function.named_parameters["session_token"] = LogicalType::VARCHAR;
	function.named_parameters["endpoint"] = LogicalType::VARCHAR;
	function.named_parameters["url_style"] = LogicalType::VARCHAR;
	function.named_parameters["use_ssl"] = LogicalType::BOOLEAN;
	function.named_parameters["url_compatibility_mode"] = LogicalType::BOOLEAN;

	if (type == "r2") {
		function.named_parameters["account_id"] = LogicalType::VARCHAR;
	}
}

void CreateS3SecretFunctions::RegisterCreateSecretFunction(DatabaseInstance &instance, string type) {
	// Register the new type
	SecretType secret_type;
	secret_type.name = type;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);

	CreateSecretFunction from_empty_config_fun2 = {type, "config", CreateS3SecretFromConfig};
	SetBaseNamedParams(from_empty_config_fun2, type);
	ExtensionUtil::RegisterFunction(instance, from_empty_config_fun2);
}

void CreateBearerTokenFunctions::Register(DatabaseInstance &instance) {
	// HuggingFace secret
	SecretType secret_type_hf;
	secret_type_hf.name = HUGGINGFACE_TYPE;
	secret_type_hf.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type_hf.default_provider = "config";
	ExtensionUtil::RegisterSecretType(instance, secret_type_hf);

	// Huggingface config provider
	CreateSecretFunction hf_config_fun = {HUGGINGFACE_TYPE, "config", CreateBearerSecretFromConfig};
	hf_config_fun.named_parameters["token"] = LogicalType::VARCHAR;
	ExtensionUtil::RegisterFunction(instance, hf_config_fun);

	// Huggingface credential_chain provider
	CreateSecretFunction hf_cred_fun = {HUGGINGFACE_TYPE, "credential_chain",
	                                    CreateHuggingFaceSecretFromCredentialChain};
	ExtensionUtil::RegisterFunction(instance, hf_cred_fun);
}

unique_ptr<BaseSecret> CreateBearerTokenFunctions::CreateSecretFunctionInternal(ClientContext &context,
                                                                                CreateSecretInput &input,
                                                                                const string &token) {
	// Set scope to user provided scope or the default
	auto scope = input.scope;
	if (scope.empty()) {
		if (input.type == HUGGINGFACE_TYPE) {
			scope.push_back("hf://");
		} else {
			throw InternalException("Unknown secret type found in httpfs extension: '%s'", input.type);
		}
	}
	auto return_value = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	//! Set key value map
	return_value->secret_map["token"] = token;

	//! Set redact keys
	return_value->redact_keys = {"token"};

	return std::move(return_value);
}

unique_ptr<BaseSecret> CreateBearerTokenFunctions::CreateBearerSecretFromConfig(ClientContext &context,
                                                                                CreateSecretInput &input) {
	string token;

	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);
		if (lower_name == "token") {
			token = named_param.second.ToString();
		}
	}

	return CreateSecretFunctionInternal(context, input, token);
}

static string TryReadTokenFile(const string &token_path, const string error_source_message,
                               bool fail_on_exception = true) {
	try {
		LocalFileSystem fs;
		auto handle = fs.OpenFile(token_path, {FileOpenFlags::FILE_FLAGS_READ});
		return handle->ReadLine();
	} catch (std::exception &ex) {
		if (!fail_on_exception) {
			return "";
		}
		ErrorData error(ex);
		throw IOException("Failed to read token path '%s'%s. (error: %s)", token_path, error_source_message,
		                  error.RawMessage());
	}
}

unique_ptr<BaseSecret>
CreateBearerTokenFunctions::CreateHuggingFaceSecretFromCredentialChain(ClientContext &context,
                                                                       CreateSecretInput &input) {
	// Step 1: Try the ENV variable HF_TOKEN
	const char *hf_token_env = std::getenv("HF_TOKEN");
	if (hf_token_env) {
		return CreateSecretFunctionInternal(context, input, hf_token_env);
	}
	// Step 2: Try the ENV variable HF_TOKEN_PATH
	const char *hf_token_path_env = std::getenv("HF_TOKEN_PATH");
	if (hf_token_path_env) {
		auto token = TryReadTokenFile(hf_token_path_env, " fetched from HF_TOKEN_PATH env variable");
		return CreateSecretFunctionInternal(context, input, token);
	}

	// Step 3: Try the path $HF_HOME/token
	const char *hf_home_env = std::getenv("HF_HOME");
	if (hf_home_env) {
		auto token_path = LocalFileSystem().JoinPath(hf_home_env, "token");
		auto token = TryReadTokenFile(token_path, " constructed using the HF_HOME variable: '$HF_HOME/token'");
		return CreateSecretFunctionInternal(context, input, token);
	}

	// Step 4: Check the default path
	auto token = TryReadTokenFile("~/.cache/huggingface/token", "", false);
	return CreateSecretFunctionInternal(context, input, token);
}
} // namespace duckdb
