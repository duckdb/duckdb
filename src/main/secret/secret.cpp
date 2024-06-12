#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/pair.hpp"

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
	return ToMapValue().ToString();
}

Value BaseSecret::ToMapValue(SecretDisplayType display_type) const {
	return Value();
}

Value BaseSecret::ToMapValueShort(SecretDisplayType display_type) const {
	return Value();
}

void BaseSecret::Serialize(Serializer &serializer) const {
	throw InternalException("Attempted to serialize secret without serialize");
}

Value KeyValueSecret::ToMapValue(SecretDisplayType mode) const {
	vector<Value> keys;
	vector<Value> values;

	// Name
	keys.push_back("name");
	values.push_back(name);

	// Type
	keys.push_back("type");
	values.push_back(type);

	// Provider
	keys.push_back("provider");
	values.push_back(provider);

	// Serializable
	keys.push_back("serializable");
	values.push_back((serializable ? "true" : "false"));

	// Scope
	keys.push_back("scope");
	string scope_string;
	for (const auto &scope_it : prefix_paths) {
		scope_string += scope_it + ",";
	}
	scope_string = scope_string.substr(0, scope_string.size() - 1);
	values.push_back(scope_string);

	// Contents of secret_map
	for (auto it = secret_map.begin(); it != secret_map.end(); it++) {
		keys.push_back(it->first);
		if (mode == SecretDisplayType::REDACTED && redact_keys.find(it->first) != redact_keys.end()) {
			values.push_back("redacted");
		} else {
			values.push_back(it->second.ToString());
		}
	}

	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, keys, values);
}

Value KeyValueSecret::ToMapValueShort(SecretDisplayType mode) const {
	vector<Value> keys;
	vector<Value> values;

	// Contents of secret_map
	for (auto it = secret_map.begin(); it != secret_map.end(); it++) {
		// We only print keys that have a value to print in the short version
		if (it->second.ToString().empty()) {
			continue;
		}

		keys.push_back(it->first);
		if (mode == SecretDisplayType::REDACTED && redact_keys.find(it->first) != redact_keys.end()) {
			values.push_back("redacted");
		} else {
			values.push_back(it->second.ToString());
		}
	}

	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, keys, values);
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
