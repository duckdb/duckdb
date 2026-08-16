// A secret provider whose values are derived rather than configured, used to test the transient
// secret value machinery. Each derivation hands out a token that has never been seen before, so a
// test can tell a value that was re-derived from one that was persisted or cached.

#include "duckdb.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

using namespace duckdb; // NOLINT

namespace {

//! Number of times the provider has produced a token, over the lifetime of the process
atomic<int64_t> derivation_count {0};
//! When set, the provider throws instead of producing a token
atomic<bool> derivation_fails {false};

constexpr const char *SECRET_TYPE = "refresh_demo";
constexpr const char *SECRET_PROVIDER = "counter";

//! The recipe is stored as a STRUCT of the named parameters, the user passes it as a MAP
Value MapToStruct(const Value &map) {
	child_list_t<Value> struct_fields;
	for (const auto &kv_child : MapValue::GetChildren(map)) {
		auto kv_pair = StructValue::GetChildren(kv_child);
		struct_fields.emplace_back(kv_pair[0].ToString(), kv_pair[1]);
	}
	return Value::STRUCT(struct_fields);
}

unique_ptr<BaseSecret> CreateRefreshDemoSecret(ClientContext &context, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	for (const auto &option : input.options) {
		auto key = Identifier(option.first);
		secret->secret_map[key] = key == "refresh_info" ? MapToStruct(option.second) : option.second;
	}

	if (derivation_fails) {
		throw InvalidConfigurationException("refresh_demo: derivation failed on purpose");
	}

	// The token is what a real provider would fetch from a credential backend: it is declared
	// transient, so it is never written to the secret storage and is re-derived instead.
	auto token = StringUtil::Format("token_%d", ++derivation_count);
	secret->secret_map["token"] = Value(token);
	secret->transient_keys = {"token"};

	return std::move(secret);
}

//! Returns how many tokens the provider has produced so far
void DerivationCountFun(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value::BIGINT(derivation_count), count_t(args.size()));
}

//! Makes the provider throw on the next derivation, so that a failing refresh can be tested
void SetDerivationFailsFun(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<bool, bool>(args.data[0], result, args.size(), [&](bool fails) {
		derivation_fails = fails;
		return fails;
	});
}

//! Marks a secret's transient values as rejected, the way a consumer does when a backend refuses
//! the credentials it was handed
void InvalidateSecretFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t path) {
		auto &secret_manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		auto match = secret_manager.LookupSecret(transaction, path.GetString(), SECRET_TYPE);
		if (!match.HasMatch()) {
			return false;
		}
		match.GetSecret().Cast<KeyValueSecret>().MarkMustRefresh();
		return true;
	});
}

} // namespace

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(loadable_secret_refresh_demo, loader) {
	SecretType secret_type;
	secret_type.name = SECRET_TYPE;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = SECRET_PROVIDER;
	loader.RegisterSecretType(secret_type);

	CreateSecretFunction secret_function;
	secret_function.secret_type = SECRET_TYPE;
	secret_function.provider = SECRET_PROVIDER;
	secret_function.function = CreateRefreshDemoSecret;
	secret_function.named_parameters["refresh_info"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	secret_function.named_parameters["account"] = LogicalType::VARCHAR;
	loader.RegisterFunction(secret_function);

	loader.RegisterFunction(
	    ScalarFunction("refresh_demo_derivations", {}, LogicalType::BIGINT, DerivationCountFun));
	loader.RegisterFunction(
	    ScalarFunction("refresh_demo_set_fails", {LogicalType::BOOLEAN}, LogicalType::BOOLEAN, SetDerivationFailsFun));
	loader.RegisterFunction(
	    ScalarFunction("refresh_demo_invalidate", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, InvalidateSecretFun));
}
}
