#pragma once

#include "duckdb.hpp"

namespace duckdb {
struct CreateSecretInput;
struct S3AuthParams;
class CreateSecretFunction;
class BaseSecret;

struct CreateS3SecretFunctions {
public:
	//! Register all CreateSecretFunctions
	static void Register(DatabaseInstance &instance);

protected:
	//! Internal function to create BaseSecret from S3AuthParams
	static unique_ptr<BaseSecret> CreateSecretFunctionInternal(ClientContext &context, CreateSecretInput &input);

	//! Function for the "settings" provider: creates secret from current duckdb settings
	static unique_ptr<BaseSecret> CreateS3SecretFromSettings(ClientContext &context, CreateSecretInput &input);
	//! Function for the "config" provider: creates secret from parameters passed by user
	static unique_ptr<BaseSecret> CreateS3SecretFromConfig(ClientContext &context, CreateSecretInput &input);

	//! Helper function to set named params of secret function
	static void SetBaseNamedParams(CreateSecretFunction &function, string &type);
	//! Helper function to create secret types s3/r2/gcs
	static void RegisterCreateSecretFunction(DatabaseInstance &instance, string type);
};

struct CreateBearerTokenFunctions {
public:
	static constexpr const char *HUGGINGFACE_TYPE = "huggingface";

	//! Register all CreateSecretFunctions
	static void Register(DatabaseInstance &instance);

protected:
	//! Internal function to create bearer token
	static unique_ptr<BaseSecret> CreateSecretFunctionInternal(ClientContext &context, CreateSecretInput &input,
	                                                           const string &token);
	//! Function for the "config" provider: creates secret from parameters passed by user
	static unique_ptr<BaseSecret> CreateBearerSecretFromConfig(ClientContext &context, CreateSecretInput &input);
	//! Function for the "config" provider: creates secret from parameters passed by user
	static unique_ptr<BaseSecret> CreateHuggingFaceSecretFromCredentialChain(ClientContext &context,
	                                                                         CreateSecretInput &input);
};

} // namespace duckdb
