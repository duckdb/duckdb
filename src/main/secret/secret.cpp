#include "duckdb/main/secret/secret.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

int64_t BaseSecret::MatchScore(const string &path) const {
	int64_t longest_match = NumericLimits<int64_t>::Minimum();
	for (const auto &prefix : prefix_paths) {
		// Handle empty scope which matches all at lowest possible score
		if (prefix.empty()) {
			longest_match = 0;
			continue;
		}
		if (StringUtil::StartsWith(path, prefix)) {
			longest_match = MaxValue<int64_t>(NumericCast<int64_t>(prefix.length()), longest_match);
		}
	}
	return longest_match;
}

void BaseSecret::SerializeBaseSecret(Serializer &serializer) const {
	serializer.WriteProperty(100, "type", type);
	serializer.WriteProperty(101, "provider", provider);
	serializer.WriteProperty(102, "name", name);
	serializer.WriteList(103, "scope", prefix_paths.size(),
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(prefix_paths[i]); });
}

string BaseSecret::ToString(SecretDisplayType display_type) const {
	return "";
}

void BaseSecret::Serialize(Serializer &serializer) const {
	throw InternalException("Attempted to serialize secret without serialize");
}

string KeyValueSecret::ToString(SecretDisplayType mode) const {
	string result;

	result += "name=" + name + ";";
	result += "type=" + type + ";";
	result += "provider=" + provider + ";";
	result += string("serializable=") + (serializable ? "true" : "false") + ";";
	result += "scope=";
	for (const auto &scope_it : prefix_paths) {
		result += scope_it + ",";
	}
	result = result.substr(0, result.size() - 1);
	result += ";";
	for (auto it = secret_map.begin(); it != secret_map.end(); it++) {
		result.append(it->first);
		result.append("=");
		if (mode == SecretDisplayType::REDACTED && redact_keys.find(it->first) != redact_keys.end()) {
			result.append("redacted");
		} else {
			result.append(it->second.ToString());
		}
		if (it != --secret_map.end()) {
			result.append(";");
		}
	}

	return result;
}

// FIXME: use serialization scripts
void KeyValueSecret::Serialize(Serializer &serializer) const {
	BaseSecret::SerializeBaseSecret(serializer);

	vector<Value> map_values;
	for (auto it = secret_map.begin(); it != secret_map.end(); it++) {
		child_list_t<Value> map_struct;
		map_struct.push_back(make_pair("key", Value(it->first)));
		map_struct.push_back(make_pair("value", Value(it->second)));
		map_values.push_back(Value::STRUCT(map_struct));
	}

	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	auto map = Value::MAP(ListType::GetChildType(map_type), map_values);
	serializer.WriteProperty(201, "secret_map", map);

	vector<Value> redact_key_values;
	for (auto it = redact_keys.begin(); it != redact_keys.end(); it++) {
		redact_key_values.push_back(*it);
	}
	auto list = Value::LIST(LogicalType::VARCHAR, redact_key_values);
	serializer.WriteProperty(202, "redact_keys", list);
}

Value KeyValueSecret::TryGetValue(const string &key, bool error_on_missing) const {
	auto lookup = secret_map.find(key);
	if (lookup == secret_map.end()) {
		if (error_on_missing) {
			throw InternalException("Failed to fetch key '%s' from secret '%s' of type '%s'", key, name, type);
		}
		return Value();
	}

	return lookup->second;
}

KeyValueSecretReader::KeyValueSecretReader(FileOpener &opener_p, FileOpenerInfo &info, const char *secret_type)
    : KeyValueSecretReader(opener_p, info, &secret_type, 1) {
}

KeyValueSecretReader::KeyValueSecretReader(FileOpener &opener_p, FileOpenerInfo &info, const char **secret_types,
                                           idx_t secret_types_len)
    : opener(opener_p), opener_info(info) {
	auto db = opener->TryGetDatabase();
	if (!db) {
		return;
	}
	auto &secret_manager = db->GetSecretManager();
	auto context = opener->TryGetClientContext();
	auto transaction = context ? CatalogTransaction::GetSystemCatalogTransaction(*context)
	                           : CatalogTransaction::GetSystemTransaction(*db);

	SecretMatch secret_match;
	for (idx_t i = 0; i < secret_types_len; i++) {
		auto &secret_type = secret_types[i];
		secret_match = secret_manager.LookupSecret(transaction, info.file_path, secret_type);
		if (secret_match.HasMatch()) {
			break;
		}
	}

	if (secret_match.HasMatch()) {
		secret = dynamic_cast<const KeyValueSecret &>(secret_match.GetSecret());
		secret_entry = std::move(secret_match.secret_entry);
	}
}

KeyValueSecretReader::~KeyValueSecretReader() {
}

SettingLookupResult KeyValueSecretReader::TryGetSecretKey(const string &secret_key, Value &result) {
	if (secret && secret->TryGetValue(secret_key, result)) {
		return SettingLookupResult(SettingScope::SECRET);
	}
	return SettingLookupResult();
}

SettingLookupResult KeyValueSecretReader::TryGetSecretKeyOrSetting(const string &secret_key, const string &setting_name,
                                                                   Value &result) {
	if (secret && secret->TryGetValue(secret_key, result)) {
		return SettingLookupResult(SettingScope::SECRET);
	}
	if (opener) {
		auto res = opener->TryGetCurrentSetting(setting_name, result);
		if (res) {
			return res;
		}
	}
	return SettingLookupResult();
}

Value KeyValueSecretReader::GetSecretKey(const string &secret_key) {
	Value result;
	if (TryGetSecretKey(secret_key, result)) {
		return result;
	}
	ThrowNotFoundError(secret_key);
}

Value KeyValueSecretReader::GetSecretKeyOrSetting(const string &secret_key, const string &setting_name) {
	Value result;
	if (TryGetSecretKeyOrSetting(secret_key, setting_name, result)) {
		return result;
	}
	ThrowNotFoundError(secret_key, setting_name);
}

void KeyValueSecretReader::ThrowNotFoundError(const string &secret_key) {
	string base_message = "Failed to fetch required secret key '%s' from secret";

	if (!secret) {
		string secret_scope = opener_info ? opener_info->file_path : "";
		string secret_scope_hint_message = secret_scope.empty() ? "." : " for '" + secret_scope + "'.";
		throw InvalidConfigurationException(base_message + ", because no secret was found%s", secret_key,
		                                    secret_scope_hint_message);
	}

	throw InvalidConfigurationException(base_message + " '%s'.", secret_key, secret->GetName());
}

void KeyValueSecretReader::ThrowNotFoundError(const string &secret_key, const string &setting_name) {
	string base_message = "Failed to fetch a parameter from either the secret key '%s' or the setting '%s'";

	if (!secret) {
		string secret_scope = opener_info ? opener_info->file_path : "";
		string secret_scope_hint_message = secret_scope.empty() ? "." : " for '" + secret_scope + "'.";
		throw InvalidConfigurationException(base_message + ": no secret was found%s", secret_key, setting_name,
		                                    secret_scope_hint_message);
	}

	throw InvalidConfigurationException(base_message +
	                                        ": secret '%s' did not contain the key, also the setting was not found.",
	                                    secret_key, setting_name, secret->GetName());
}

bool CreateSecretFunctionSet::ProviderExists(const string &provider_name) {
	return functions.find(provider_name) != functions.end();
}

void CreateSecretFunctionSet::AddFunction(CreateSecretFunction &function, OnCreateConflict on_conflict) {
	if (ProviderExists(function.provider)) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw InternalException(
			    "Attempted to override a Create Secret Function with OnCreateConflict::ERROR_ON_CONFLICT for: '%s'",
			    function.provider);
		} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			functions[function.provider] = function;
		} else if (on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
			throw NotImplementedException("ALTER_ON_CONFLICT not implemented for CreateSecretFunctionSet");
		}
	} else {
		functions[function.provider] = function;
	}
}

CreateSecretFunction &CreateSecretFunctionSet::GetFunction(const string &provider) {
	const auto &lookup = functions.find(provider);

	if (lookup == functions.end()) {
		throw InternalException("Could not find Create Secret Function with provider %s");
	}

	return lookup->second;
}

} // namespace duckdb
