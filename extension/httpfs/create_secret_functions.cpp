#include "create_secret_functions.hpp"
#include "s3fs.hpp"
#include "duckdb/function/create_secret_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

void CreateS3SecretFunctions::Register(DatabaseInstance &instance) {
	RegisterCreateSecretFunction(instance, "s3");
	RegisterCreateSecretFunction(instance, "r2");
	RegisterCreateSecretFunction(instance, "gcs");
}

shared_ptr<RegisteredSecret> CreateS3SecretFunctions::CreateSecretFunctionInternal(ClientContext &context, CreateSecretInput& input, S3AuthParams params) {
	// for r2 we can set the endpoint using the account id
	if (input.type == "r2" && input.named_parameters.find("account_id") != input.named_parameters.end()) {
		params.endpoint = input.named_parameters["account_id"].ToString() + ".r2.cloudflarestorage.com";
	}

	// apply any overridden settings
	for(const auto& named_param : input.named_parameters) {
		if (named_param.first == "key_id") {
			params.access_key_id = named_param.second.ToString();
		} else if (named_param.first == "secret") {
			params.secret_access_key = named_param.second.ToString();
		} else if (named_param.first == "region") {
			params.region = named_param.second.ToString();
		} else if (named_param.first == "session_token") {
			params.session_token = named_param.second.ToString();
		} else if (named_param.first == "endpoint") {
			params.endpoint = named_param.second.ToString();
		} else if (named_param.first == "url_style") {
			params.url_style = named_param.second.ToString();
		} else if (named_param.first == "use_ssl") {
			params.url_style = named_param.second.GetValue<bool>();
		} else if (named_param.first == "url_compatibility_mode") {
			params.s3_url_compatibility_mode = named_param.second.GetValue<bool>();
		} else if (named_param.first == "account_id") {
			continue; // handled already
		} else {
			throw InternalException("Unknown named parameter passed to CreateSecretFunctionInternal: " + named_param.first);
		}
	}

	// Set scope to user provided scope or the default
	auto scope = input.scope;
	if (scope.empty()) {
		if (input.type == "s3") {
			scope.push_back("s3://");
		} else if (input.type == "r2") {
			scope.push_back("s3://"); // TODO make this r2://
		} else if (input.type == "gcs") {
			scope.push_back("s3://"); // TODO make this gcs://
		} else {
			throw InternalException("Unknown secret type found in httpfs extension: '%s'", input.type);
		}
	}

	auto secret = make_shared<S3Secret>(scope, input.type, input.provider, input.name, params);
	return secret;
}

shared_ptr<RegisteredSecret> CreateS3SecretFunctions::CreateS3SecretFromSettings(ClientContext &context, CreateSecretInput& input) {
	auto& opener = context.client_data->file_opener;
	FileOpenerInfo info;
	auto params = S3AuthParams::ReadFrom(opener.get(), info);
	return CreateSecretFunctionInternal(context, input, params);
}

shared_ptr<RegisteredSecret> CreateS3SecretFunctions::CreateS3SecretFromConfig(ClientContext &context, CreateSecretInput& input) {
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
	function.named_parameters["use_ssl"] = LogicalType::VARCHAR;
	function.named_parameters["url_compatibility_mode"] = LogicalType::VARCHAR;

	if (type == "r2") {
		function.named_parameters["account_id"] = LogicalType::VARCHAR;
	}
}

void CreateS3SecretFunctions::RegisterCreateSecretFunction(DatabaseInstance &instance, string type) {
	// Register the new type
	ExtensionUtil::RegisterSecretType(instance, {type, nullptr});

	// Default function
	auto default_fun = CreateSecretFunction(type, "", CreateS3SecretFromConfig);
	SetBaseNamedParams(default_fun, type);
	ExtensionUtil::RegisterFunction(instance, default_fun);

	//! Create from config
	CreateSecretFunction from_empty_config_fun(type, "config", CreateS3SecretFromConfig);
	SetBaseNamedParams(from_empty_config_fun, type);
	ExtensionUtil::AddFunctionOverload(instance, from_empty_config_fun);

	//! Create from empty config
	CreateSecretFunction from_settings_fun(type, "duckdb_settings", CreateS3SecretFromSettings);
	SetBaseNamedParams(from_settings_fun, type);
	ExtensionUtil::AddFunctionOverload(instance, from_settings_fun);
}
} // namespace duckdb
