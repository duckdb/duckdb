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
                                                                             CreateSecretInput &input,
                                                                             S3AuthParams params) {
	// for r2 we can set the endpoint using the account id
	if (input.type == "r2" && input.options.find("account_id") != input.options.end()) {
		params.endpoint = input.options["account_id"].ToString() + ".r2.cloudflarestorage.com";
	}

	// apply any overridden settings
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "key_id") {
			params.access_key_id = named_param.second.ToString();
		} else if (lower_name == "secret") {
			params.secret_access_key = named_param.second.ToString();
		} else if (lower_name == "region") {
			params.region = named_param.second.ToString();
		} else if (lower_name == "session_token") {
			params.session_token = named_param.second.ToString();
		} else if (lower_name == "endpoint") {
			params.endpoint = named_param.second.ToString();
		} else if (lower_name == "url_style") {
			params.url_style = named_param.second.ToString();
		} else if (lower_name == "use_ssl") {
			if (named_param.second.type() != LogicalType::BOOLEAN) {
				throw InvalidInputException("Invalid type past to secret option: '%s', found '%s', expected: 'BOOLEAN'",
				                            lower_name, named_param.second.type().ToString());
			}
			params.use_ssl = named_param.second.GetValue<bool>();
		} else if (lower_name == "url_compatibility_mode") {
			if (named_param.second.type() != LogicalType::BOOLEAN) {
				throw InvalidInputException("Invalid type past to secret option: '%s', found '%s', expected: 'BOOLEAN'",
				                            lower_name, named_param.second.type().ToString());
			}
			params.s3_url_compatibility_mode = named_param.second.GetValue<bool>();
		} else if (lower_name == "account_id") {
			continue; // handled already
		} else {
			throw InternalException("Unknown named parameter passed to CreateSecretFunctionInternal: " + lower_name);
		}
	}

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

	return S3SecretHelper::CreateSecret(scope, input.type, input.provider, input.name, params);
}

unique_ptr<BaseSecret> CreateS3SecretFunctions::CreateS3SecretFromSettings(ClientContext &context,
                                                                           CreateSecretInput &input) {
	auto &opener = context.client_data->file_opener;
	FileOpenerInfo info;
	auto params = S3AuthParams::ReadFrom(opener.get(), info);
	return CreateSecretFunctionInternal(context, input, params);
}

unique_ptr<BaseSecret> CreateS3SecretFunctions::CreateS3SecretFromConfig(ClientContext &context,
                                                                         CreateSecretInput &input) {
	S3AuthParams empty_params;
	empty_params.use_ssl = true;
	empty_params.s3_url_compatibility_mode = false;
	empty_params.region = "us-east-1";
	empty_params.endpoint = "s3.amazonaws.com";

	if (input.type == "gcs") {
		empty_params.endpoint = "storage.googleapis.com";
	}

	if (input.type == "gcs" || input.type == "r2") {
		empty_params.url_style = "path";
	}

	return CreateSecretFunctionInternal(context, input, empty_params);
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
	CreateSecretFunction from_settings_fun2 = {type, "duckdb_settings", CreateS3SecretFromSettings};
	SetBaseNamedParams(from_empty_config_fun2, type);
	SetBaseNamedParams(from_settings_fun2, type);
	ExtensionUtil::RegisterFunction(instance, from_empty_config_fun2);
	ExtensionUtil::RegisterFunction(instance, from_settings_fun2);
}

void CreateBearerTokenFunctions::Register(DatabaseInstance &instance) {
	// Generic Bearer secret
	SecretType secret_type;
	secret_type.name = GENERIC_BEARER_TYPE;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	ExtensionUtil::RegisterSecretType(instance, secret_type);

	// Generic Bearer config provider
	CreateSecretFunction config_fun = {GENERIC_BEARER_TYPE, "config", CreateBearerSecretFromConfig};
	config_fun.named_parameters["token"] = LogicalType::VARCHAR;
	ExtensionUtil::RegisterFunction(instance, config_fun);

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

	// Huggingface cache provider
	CreateSecretFunction hf_cache_fun = {HUGGINGFACE_TYPE, "cache", CreateHuggingFaceSecretFromCache};
	ExtensionUtil::RegisterFunction(instance, hf_cache_fun);
}

unique_ptr<BaseSecret> CreateBearerTokenFunctions::CreateSecretFunctionInternal(ClientContext &context, CreateSecretInput &input, const string &token) {
	// Set scope to user provided scope or the default
	auto scope = input.scope;
	if (scope.empty()) {
		if (input.type == GENERIC_BEARER_TYPE) {
			scope.push_back("");
		} else if (input.type == HUGGINGFACE_TYPE) {
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

	auto token_input = input.options.find("token");
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);
		if (lower_name == "token") {
			token = named_param.second.ToString();
		}
	}

	return CreateSecretFunctionInternal(context, input, token);
}

unique_ptr<BaseSecret> CreateBearerTokenFunctions::CreateHuggingFaceSecretFromCache(ClientContext &context, CreateSecretInput &input) {
	LocalFileSystem fs;

	auto handle = fs.OpenFile("~/.cache/huggingface/token", {FileOpenFlags::FILE_FLAGS_READ});
	auto token = handle->ReadLine();

	return CreateSecretFunctionInternal(context, input, token);
}
} // namespace duckdb
