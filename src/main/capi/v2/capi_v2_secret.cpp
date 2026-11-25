#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb::capiv2 {

//! A key/value secret as the C API sees it: properties, scopes and keys, either owned and freely changeable, read-only,
//! or the output of a create callback (whose properties are fixed by the statement).
class CV2Secret {
public:
	enum class Mode { OWNED, READ_ONLY, OUTPUT };

	explicit CV2Secret(Mode mode = Mode::OWNED) : mode(mode) {
	}

	//! A copy of a stored secret, owned by the caller.
	static unique_ptr<CV2Secret> FromEntry(const SecretEntry &entry) {
		auto &kv_secret = entry.secret->Cast<KeyValueSecret>();
		auto result = make_uniq<CV2Secret>();
		result->scope = kv_secret.GetScope();
		result->type = kv_secret.GetType();
		result->provider = kv_secret.GetProvider();
		result->name = kv_secret.GetName();
		result->storage = entry.storage_mode;
		result->persist_type = entry.persist_type;
		result->values = kv_secret.secret_map;
		result->redacted = kv_secret.redact_keys;
		return result;
	}

	//! The statement being run, with its options as keys.
	static unique_ptr<CV2Secret> FromInput(const CreateSecretInput &input, Mode mode) {
		auto result = make_uniq<CV2Secret>(mode);
		result->scope = input.scope;
		result->type = input.type;
		result->provider = input.provider;
		result->name = input.name;
		result->storage = input.storage_type.GetIdentifierName();
		result->persist_type = input.persist_type;
		return result;
	}

	void CheckWritable(bool is_property) const {
		if (mode == Mode::READ_ONLY) {
			throw InvalidInputException("The secret is read-only.");
		}
		if (mode == Mode::OUTPUT && is_property) {
			throw InvalidInputException(
			    "The properties of the secret a create callback fills are fixed by the statement.");
		}
	}

	static std::string_view PersistTypeName(SecretPersistType persist_type) {
		switch (persist_type) {
		case SecretPersistType::TEMPORARY:
			return "temporary";
		case SecretPersistType::PERSISTENT:
			return "persistent";
		case SecretPersistType::TRANSACTION:
			return "transaction";
		default:
			return "default";
		}
	}

	static SecretPersistType ParsePersistType(std::string_view name) {
		auto lower = StringUtil::Lower(string(name));
		if (lower == "default") {
			return SecretPersistType::DEFAULT;
		}
		if (lower == "temporary") {
			return SecretPersistType::TEMPORARY;
		}
		if (lower == "persistent") {
			return SecretPersistType::PERSISTENT;
		}
		if (lower == "transaction") {
			return SecretPersistType::TRANSACTION;
		}
		throw InvalidInputException(
		    "Unknown persist type '%s', expected default, temporary, persistent or transaction.", string(name));
	}

	unique_ptr<KeyValueSecret> ToKeyValueSecret() const {
		auto result = make_uniq<KeyValueSecret>(scope, type, provider, name);
		result->secret_map = values;
		result->redact_keys = redacted;
		return result;
	}

public:
	Mode mode;
	vector<string> scope;
	Identifier type;
	Identifier provider;
	Identifier name;
	string storage;
	SecretPersistType persist_type = SecretPersistType::DEFAULT;
	identifier_tree_t<Value> values;
	identifier_set_t redacted;
};

static auto Convert(duckdb_v2_secret_handle secret) -> CV2Secret * {
	return reinterpret_cast<CV2Secret *>(secret);
}
static auto Convert(CV2Secret *secret) -> duckdb_v2_secret_handle {
	return reinterpret_cast<duckdb_v2_secret_handle>(secret);
}

//! The create callback of a secret function, with the user data and parameters it was registered with.
class CV2CreateSecretInfo final : public CreateSecretFunctionInfo {
public:
	duckdb_v2_secret_create_callback_fn callback = nullptr;
	shared_ptr<CV2UserData> user_data;
	named_parameter_type_map_t named_parameters;
};

static unique_ptr<BaseSecret> CallCreateCallback(ClientContext &context, CreateSecretInput &input) {
	D_ASSERT(input.function_info);
	auto &info = static_cast<CV2CreateSecretInfo &>(*input.function_info);

	// the options are cast to the declared types, which a statement has been through already
	auto in_secret = CV2Secret::FromInput(input, CV2Secret::Mode::READ_ONLY);
	for (auto &option : input.options) {
		auto param = info.named_parameters.find(Identifier(option.first));
		if (param == info.named_parameters.end()) {
			throw InvalidInputException("Unknown parameter %s for secret type %s with provider %s", option.first,
			                            input.type, input.provider);
		}
		in_secret->values[param->first] = option.second.CastAs(context, param->second);
	}
	auto out_secret = CV2Secret::FromInput(input, CV2Secret::Mode::OUTPUT);

	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	CV2CallbackContext callback_context(context);
	info.callback(Convert(in_secret.get()), Convert(out_secret.get()),
	              info.user_data ? info.user_data->GetData() : nullptr, Convert(&callback_context), &err_ptr);
	if (err.HasError()) {
		err.ThrowAsException();
	}
	return out_secret->ToKeyValueSecret();
}

class CV2SecretFunction {
public:
	explicit CV2SecretFunction(DatabaseInstance &db) : db(db) {
	}

	void Register(DatabaseInstance &target, const string &extension_name) {
		CheckRegistrationTarget(db, target, "secret function");
		if (type.empty()) {
			throw InvalidInputException("A type must be set for the secret function.");
		}
		if (provider.empty()) {
			throw InvalidInputException("A provider must be set for the secret function.");
		}
		if (!callback) {
			throw InvalidInputException("A create callback must be set for the secret function.");
		}

		auto &manager = *DBConfig::GetConfig(target).secret_manager;
		// a type an extension defines is loaded as CREATE SECRET would load it, never defined here
		auto owner = ExtensionHelper::FindExtensionInEntries(type, EXTENSION_SECRET_TYPES);
		if (!owner.empty() && owner != extension_name) {
			SecretType existing_type;
			if (!manager.TryLookupType(type, existing_type)) {
				throw InvalidInputException(
				    "Secret type %s is defined by the %s extension, load it before registering provider %s", type,
				    owner, provider);
			}
		}

		SecretType secret_type;
		secret_type.name = type;
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = is_default ? provider.GetIdentifierName() : string();
		secret_type.extension = extension_name;

		auto info = make_shared_ptr<CV2CreateSecretInfo>();
		info->callback = callback;
		info->user_data = user_data;
		info->named_parameters = named_parameters;
		CreateSecretFunction function;
		function.secret_type = type.GetIdentifierName();
		function.provider = provider;
		function.function = CallCreateCallback;
		function.named_parameters = named_parameters;
		function.info = std::move(info);
		manager.RegisterSecretFunctionAndType(std::move(function), secret_type);
	}

public:
	//! The database it was created for: the only one it can be registered on.
	DatabaseInstance &db;
	Identifier type;
	Identifier provider;
	bool is_default = false;
	named_parameter_type_map_t named_parameters;
	duckdb_v2_secret_create_callback_fn callback = nullptr;
	shared_ptr<CV2UserData> user_data;
};

static auto Convert(duckdb_v2_secret_function_handle function) -> CV2SecretFunction * {
	return reinterpret_cast<CV2SecretFunction *>(function);
}
static auto Convert(CV2SecretFunction *function) -> duckdb_v2_secret_function_handle {
	return reinterpret_cast<duckdb_v2_secret_function_handle>(function);
}

static OnCreateConflict ConvertOnConflict(DUCKDB_V2_SECRET_ON_CONFLICT on_conflict) {
	switch (on_conflict) {
	case DUCKDB_V2_SECRET_ON_CONFLICT_ERROR:
		return OnCreateConflict::ERROR_ON_CONFLICT;
	case DUCKDB_V2_SECRET_ON_CONFLICT_IGNORE:
		return OnCreateConflict::IGNORE_ON_CONFLICT;
	case DUCKDB_V2_SECRET_ON_CONFLICT_REPLACE:
		return OnCreateConflict::REPLACE_ON_CONFLICT;
	default:
		throw InvalidInputException("Unknown conflict behavior.");
	}
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

//-------------------------------------------------------- Secret function ---------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_secret_function_create(duckdb_v2_factory_handle factory,
                                                 duckdb_v2_secret_function_handle *function,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(function);
	*function = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2SecretFunction>(Convert(factory)->GetDatabase());
		*function = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_function_set_type(duckdb_v2_secret_function_handle function, const char *type,
                                                   idx_t type_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_STR_ARG(type, type_length);
	return WithErrorHandler(
	    err, [&]() { Convert(function)->type = duckdb::Identifier(ConvertIdentifierName(type, type_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_secret_function_set_provider(duckdb_v2_secret_function_handle function, const char *provider,
                                                       idx_t provider_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_STR_ARG(provider, provider_length);
	return WithErrorHandler(err, [&]() {
		Convert(function)->provider = duckdb::Identifier(ConvertIdentifierName(provider, provider_length));
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_function_set_default(duckdb_v2_secret_function_handle function, bool is_default,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->is_default = is_default; });
}

DUCKDB_V2_ERROR duckdb_v2_secret_function_add_named_parameter(duckdb_v2_secret_function_handle function,
                                                              const char *name, idx_t name_length,
                                                              duckdb_v2_logical_type_handle type,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(type);
	return WithErrorHandler(err, [&]() {
		auto parameter_type = Convert(type);
		Convert(function)->named_parameters[duckdb::Identifier(ConvertIdentifierName(name, name_length))] =
		    *parameter_type;
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_function_set_create_callback(duckdb_v2_secret_function_handle function,
                                                              duckdb_v2_secret_create_callback_fn callback,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->callback = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_secret_function_set_user_data(duckdb_v2_secret_function_handle function,
                                                        duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(function);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(function)->user_data = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_connection_register_secret_function(duckdb_v2_connection_handle conn,
                                                              duckdb_v2_secret_function_handle function,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() { Convert(function)->Register(*Convert(conn)->context->db, duckdb::string()); });
}

DUCKDB_V2_ERROR duckdb_v2_extension_register_secret_function(duckdb_v2_extension_handle extension,
                                                             duckdb_v2_secret_function_handle function,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(function);
	return WithErrorHandler(err, [&]() {
		auto &loader = GetExtensionLoader(extension);
		Convert(function)->Register(loader.GetDatabaseInstance(),
		                            loader.GetRegisteredExtensionName().GetIdentifierName());
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_function_destroy(duckdb_v2_secret_function_handle *function) {
	return WithErrorHandler(nullptr, [&]() {
		if (!function) {
			return;
		}
		if (*function) {
			delete Convert(*function);
			*function = nullptr;
		}
	});
}

//-------------------------------------------------------- Building a secret -------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_secret_create(duckdb_v2_factory_handle factory, duckdb_v2_secret_handle *secret,
                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(secret);
	*secret = nullptr;
	return WithErrorHandler(err, [&]() { *secret = Convert(new CV2Secret()); });
}

DUCKDB_V2_ERROR duckdb_v2_secret_set_property(duckdb_v2_secret_handle secret, DUCKDB_V2_SECRET_PROPERTY property,
                                              const char *value, idx_t value_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_STR_ARG(value, value_length);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		self.CheckWritable(true);
		switch (property) {
		case DUCKDB_V2_SECRET_PROPERTY_TYPE:
			self.type = duckdb::Identifier(ConvertIdentifierName(value, value_length));
			break;
		case DUCKDB_V2_SECRET_PROPERTY_PROVIDER:
			self.provider = duckdb::Identifier(ConvertIdentifierName(value, value_length));
			break;
		case DUCKDB_V2_SECRET_PROPERTY_NAME:
			self.name = duckdb::Identifier(ConvertIdentifierName(value, value_length));
			break;
		case DUCKDB_V2_SECRET_PROPERTY_STORAGE:
			self.storage = duckdb::string(Convert(value, value_length));
			break;
		case DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE:
			self.persist_type = CV2Secret::ParsePersistType(Convert(value, value_length));
			break;
		default:
			throw duckdb::InvalidInputException("Unknown secret property.");
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_add_scope(duckdb_v2_secret_handle secret, const char *scope, idx_t scope_length,
                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_STR_ARG(scope, scope_length);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		self.CheckWritable(false);
		self.scope.push_back(duckdb::string(Convert(scope, scope_length)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_clear_scope(duckdb_v2_secret_handle secret, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		self.CheckWritable(false);
		self.scope.clear();
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_set_value(duckdb_v2_secret_handle secret, const char *key, idx_t key_length,
                                           duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		self.CheckWritable(false);
		self.values[duckdb::Identifier(ConvertIdentifierName(key, key_length))] = *Convert(value);
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_add_redacted_key(duckdb_v2_secret_handle secret, const char *key, idx_t key_length,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		self.CheckWritable(false);
		self.redacted.insert(duckdb::Identifier(ConvertIdentifierName(key, key_length)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_destroy(duckdb_v2_secret_handle *secret) {
	return WithErrorHandler(nullptr, [&]() {
		if (!secret) {
			return;
		}
		if (*secret) {
			delete Convert(*secret);
			*secret = nullptr;
		}
	});
}

//-------------------------------------------------------- Reading a secret --------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_secret_get_property(duckdb_v2_secret_handle secret, DUCKDB_V2_SECRET_PROPERTY property,
                                              const char **value, idx_t *value_length,
                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_ARG(value);
	DUCKDB_CHECK_ARG(value_length);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		switch (property) {
		case DUCKDB_V2_SECRET_PROPERTY_TYPE:
			SetStringOut(value, value_length, self.type);
			break;
		case DUCKDB_V2_SECRET_PROPERTY_PROVIDER:
			SetStringOut(value, value_length, self.provider);
			break;
		case DUCKDB_V2_SECRET_PROPERTY_NAME:
			SetStringOut(value, value_length, self.name);
			break;
		case DUCKDB_V2_SECRET_PROPERTY_STORAGE:
			SetStringOut(value, value_length, std::string_view(self.storage));
			break;
		case DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE:
			SetStringOut(value, value_length, CV2Secret::PersistTypeName(self.persist_type));
			break;
		default:
			throw duckdb::InvalidInputException("Unknown secret property.");
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_get_scope_count(duckdb_v2_secret_handle secret, idx_t *count,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(secret)->scope.size(); });
}

DUCKDB_V2_ERROR duckdb_v2_secret_get_scope(duckdb_v2_secret_handle secret, idx_t index, const char **scope,
                                           idx_t *scope_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_ARG(scope);
	DUCKDB_CHECK_ARG(scope_length);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		if (index >= self.scope.size()) {
			throw duckdb::OutOfRangeException("The scope index %llu is out of range.", index);
		}
		SetStringOut(scope, scope_length, std::string_view(self.scope[index]));
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_get_key_count(duckdb_v2_secret_handle secret, idx_t *count,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(secret)->values.size(); });
}

DUCKDB_V2_ERROR duckdb_v2_secret_get_key(duckdb_v2_secret_handle secret, idx_t index, const char **key,
                                         idx_t *key_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_ARG(key);
	DUCKDB_CHECK_ARG(key_length);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		if (index >= self.values.size()) {
			throw duckdb::OutOfRangeException("The key index %llu is out of range.", index);
		}
		SetStringOut(key, key_length, std::next(self.values.begin(), static_cast<int64_t>(index))->first);
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_get_value(duckdb_v2_secret_handle secret, const char *key, idx_t key_length,
                                           duckdb_v2_value_handle *value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(value);
	*value = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		auto entry = self.values.find(duckdb::Identifier(ConvertIdentifierName(key, key_length)));
		if (entry != self.values.end()) {
			*value = Convert(new duckdb::Value(entry->second));
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_secret_is_key_redacted(duckdb_v2_secret_handle secret, const char *key, idx_t key_length,
                                                 bool *redacted, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(redacted);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		auto identifier = duckdb::Identifier(ConvertIdentifierName(key, key_length));
		*redacted = self.redacted.find(identifier) != self.redacted.end() && self.values.count(identifier) > 0;
	});
}

//-------------------------------------------------------- Finding and creating secrets --------------------------------

namespace {

DUCKDB_V2_ERROR LookupSecret(CV2Context &ctx, const char *path, idx_t path_length, const char *type, idx_t type_length,
                             duckdb_v2_secret_handle *secret, int64_t *score, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_STR_ARG(path, path_length);
	DUCKDB_CHECK_STR_ARG(type, type_length);
	DUCKDB_CHECK_ARG(secret);
	*secret = nullptr;
	return WithErrorHandler(err, [&]() {
		auto path_string = duckdb::string(Convert(path, path_length));
		auto type_string = duckdb::string(ConvertIdentifierName(type, type_length));
		ctx.WithContext([&](duckdb::ClientContext &context) {
			auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
			auto match = duckdb::SecretManager::Get(context).LookupSecret(transaction, path_string, type_string);
			if (match.HasMatch()) {
				*secret = Convert(CV2Secret::FromEntry(*match.secret_entry).release());
				if (score) {
					*score = match.score;
				}
			}
		});
	});
}

DUCKDB_V2_ERROR GetSecretByName(CV2Context &ctx, const char *name, idx_t name_length, duckdb_v2_secret_handle *secret,
                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_STR_ARG(name, name_length);
	DUCKDB_CHECK_ARG(secret);
	*secret = nullptr;
	return WithErrorHandler(err, [&]() {
		auto name_string = duckdb::string(ConvertIdentifierName(name, name_length));
		ctx.WithContext([&](duckdb::ClientContext &context) {
			auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
			auto entry = duckdb::SecretManager::Get(context).GetSecretByName(transaction, name_string);
			if (entry) {
				*secret = Convert(CV2Secret::FromEntry(*entry).release());
			}
		});
	});
}

DUCKDB_V2_ERROR CreateSecret(CV2Context &ctx, duckdb_v2_secret_handle secret, DUCKDB_V2_SECRET_ON_CONFLICT on_conflict,
                             duckdb_v2_secret_handle *created, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(secret);
	if (created) {
		*created = nullptr;
	}
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(secret);
		if (self.type.empty()) {
			throw duckdb::InvalidInputException("A type must be set to create the secret.");
		}
		duckdb::CreateSecretInput input;
		input.type = self.type;
		input.provider = self.provider;
		// an unnamed secret gets the default name of its type, as CREATE SECRET without a name does
		input.name = self.name.empty()
		                 ? duckdb::Identifier("__default_" + duckdb::StringUtil::Lower(self.type.GetIdentifierName()))
		                 : self.name;
		input.scope = self.scope;
		for (auto &value : self.values) {
			input.options[value.first.GetIdentifierName()] = value.second;
		}
		input.on_conflict = ConvertOnConflict(on_conflict);
		// a looked-up transaction secret reports storage `transaction`: that is its persist type, not a storage
		auto storage = duckdb::Identifier(self.storage);
		auto persist_type = self.persist_type;
		if (storage == duckdb::SecretManager::TRANSACTION_STORAGE_NAME) {
			if (persist_type != duckdb::SecretPersistType::DEFAULT &&
			    persist_type != duckdb::SecretPersistType::TRANSACTION) {
				throw duckdb::InvalidInputException(
				    "Storage 'transaction' requires the 'transaction' persist type, not '%s'",
				    duckdb::string(CV2Secret::PersistTypeName(persist_type)));
			}
			persist_type = duckdb::SecretPersistType::TRANSACTION;
			storage = duckdb::Identifier();
		}
		input.storage_type = storage;
		input.persist_type = persist_type;
		ctx.WithContext([&](duckdb::ClientContext &context) {
			duckdb::SecretManager::ThrowIfSecretCreationDisabled(*context.db, input.type, input.provider);
			auto &manager = duckdb::SecretManager::Get(context);
			auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
			manager.BindCreateSecretOptions(context.GetContext(), transaction, input);
			auto entry = manager.CreateSecret(context, input);
			if (!created) {
				return;
			}
			if (!entry) {
				// the conflict was ignored: return the secret it was with, from the storage the request resolved to
				auto persist_type = input.persist_type;
				auto storage = manager.ResolveSecretStorage(persist_type, input.storage_type);
				entry = manager.GetSecretByName(transaction, input.name.GetIdentifierName(), storage);
			}
			if (entry) {
				*created = Convert(CV2Secret::FromEntry(*entry).release());
			}
		});
	});
}

} // namespace

DUCKDB_V2_ERROR duckdb_v2_context_lookup_secret(duckdb_v2_context_handle ctx, const char *path, idx_t path_length,
                                                const char *type, idx_t type_length, duckdb_v2_secret_handle *secret,
                                                int64_t *score, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(ctx);
	return LookupSecret(*Convert(ctx), path, path_length, type, type_length, secret, score, err);
}

DUCKDB_V2_ERROR duckdb_v2_connection_lookup_secret(duckdb_v2_connection_handle conn, const char *path,
                                                   idx_t path_length, const char *type, idx_t type_length,
                                                   duckdb_v2_secret_handle *secret, int64_t *score,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	return LookupSecret(Convert(conn)->context_handle, path, path_length, type, type_length, secret, score, err);
}

DUCKDB_V2_ERROR duckdb_v2_context_get_secret_by_name(duckdb_v2_context_handle ctx, const char *name, idx_t name_length,
                                                     duckdb_v2_secret_handle *secret,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(ctx);
	return GetSecretByName(*Convert(ctx), name, name_length, secret, err);
}

DUCKDB_V2_ERROR duckdb_v2_connection_get_secret_by_name(duckdb_v2_connection_handle conn, const char *name,
                                                        idx_t name_length, duckdb_v2_secret_handle *secret,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	return GetSecretByName(Convert(conn)->context_handle, name, name_length, secret, err);
}

DUCKDB_V2_ERROR duckdb_v2_context_create_secret(duckdb_v2_context_handle ctx, duckdb_v2_secret_handle secret,
                                                DUCKDB_V2_SECRET_ON_CONFLICT on_conflict,
                                                duckdb_v2_secret_handle *created, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(ctx);
	return CreateSecret(*Convert(ctx), secret, on_conflict, created, err);
}

DUCKDB_V2_ERROR duckdb_v2_connection_create_secret(duckdb_v2_connection_handle conn, duckdb_v2_secret_handle secret,
                                                   DUCKDB_V2_SECRET_ON_CONFLICT on_conflict,
                                                   duckdb_v2_secret_handle *created, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	return CreateSecret(Convert(conn)->context_handle, secret, on_conflict, created, err);
}
