#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "httpfs_extension.hpp"
#include "s3fs.hpp"

namespace duckdb {

//! Internal function to create RegisteredSecret from S3AuthParams
shared_ptr<RegisteredSecret> CreateSecretFunctionInternal(ClientContext &context, CreateSecretInput& input, S3AuthParams params) {
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

	auto cred = make_shared<S3Secret>(scope, input.type, input.provider, params);
	cred->SetAlias(input.name);
	return cred;
}

shared_ptr<RegisteredSecret> CreateS3SecretFromSettings(ClientContext &context, CreateSecretInput& input) {
	auto& opener = context.client_data->file_opener;
	FileOpenerInfo info;
	auto params = S3AuthParams::ReadFrom(opener.get(), info);
	return CreateSecretFunctionInternal(context, input, params);
}

shared_ptr<RegisteredSecret> CreateS3SecretFromConfig(ClientContext &context, CreateSecretInput& input) {
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

static void SetBaseNamedParams(CreateSecretFunction &function, string &type) {
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

static void RegisterCreateSecretFunction(DatabaseInstance &instance, string type) {
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

static void RegisterCreateSecretFunctions(DatabaseInstance &instance) {
	RegisterCreateSecretFunction(instance, "s3");
	RegisterCreateSecretFunction(instance, "r2");
	RegisterCreateSecretFunction(instance, "gcs");
}

static void LoadInternal(DatabaseInstance &instance) {
	S3FileSystem::Verify(); // run some tests to see if all the hashes work out
	auto &fs = instance.GetFileSystem();

	fs.RegisterSubSystem(make_uniq<HTTPFileSystem>());
	fs.RegisterSubSystem(make_uniq<S3FileSystem>(BufferManager::GetBufferManager(instance)));

	auto &config = DBConfig::GetConfig(instance);

	// Global HTTP config
	// Single timeout value is used for all 4 types of timeouts, we could split it into 4 if users need that
	config.AddExtensionOption("http_timeout", "HTTP timeout read/write/connection/retry (default 30000ms)",
	                          LogicalType::UBIGINT, Value(30000));
	config.AddExtensionOption("http_retries", "HTTP retries on I/O error (default 3)", LogicalType::UBIGINT, Value(3));
	config.AddExtensionOption("http_retry_wait_ms", "Time between retries (default 100ms)", LogicalType::UBIGINT,
	                          Value(100));
	config.AddExtensionOption("force_download", "Forces upfront download of file", LogicalType::BOOLEAN, Value(false));
	// Reduces the number of requests made while waiting, for example retry_wait_ms of 50 and backoff factor of 2 will
	// result in wait times of  0 50 100 200 400...etc.
	config.AddExtensionOption("http_retry_backoff",
	                          "Backoff factor for exponentially increasing retry wait time (default 4)",
	                          LogicalType::FLOAT, Value(4));
	// Global S3 config
	config.AddExtensionOption("s3_region", "S3 Region", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_access_key_id", "S3 Access Key ID", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_secret_access_key", "S3 Access Key", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_session_token", "S3 Session Token", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_endpoint", "S3 Endpoint (default 's3.amazonaws.com')", LogicalType::VARCHAR,
	                          Value("s3.amazonaws.com"));
	config.AddExtensionOption("s3_url_style", "S3 url style ('vhost' (default) or 'path')", LogicalType::VARCHAR,
	                          Value("vhost"));
	config.AddExtensionOption("s3_use_ssl", "S3 use SSL (default true)", LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("s3_url_compatibility_mode", "Disable Globs and Query Parameters on S3 urls",
	                          LogicalType::BOOLEAN, Value(false));

	// S3 Uploader config
	config.AddExtensionOption("s3_uploader_max_filesize",
	                          "S3 Uploader max filesize (between 50GB and 5TB, default 800GB)", LogicalType::VARCHAR,
	                          "800GB");
	config.AddExtensionOption("s3_uploader_max_parts_per_file",
	                          "S3 Uploader max parts per file (between 1 and 10000, default 10000)",
	                          LogicalType::UBIGINT, Value(10000));
	config.AddExtensionOption("s3_uploader_thread_limit", "S3 Uploader global thread limit (default 50)",
	                          LogicalType::UBIGINT, Value(50));

	auto provider = make_uniq<AWSEnvironmentCredentialsProvider>(config);
	provider->SetAll();

	RegisterCreateSecretFunctions(instance);
}

void HttpfsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string HttpfsExtension::Name() {
	return "httpfs";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void httpfs_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *httpfs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
