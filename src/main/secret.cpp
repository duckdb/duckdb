#include "duckdb/main/secret.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

int BaseSecret::MatchScore(const string &path) {
	int longest_match = -1;
	for (const auto &prefix : prefix_paths) {
		if (prefix == "*") {
			longest_match = 0;
			continue;
		}
		if (StringUtil::StartsWith(path, prefix)) {
			longest_match = MaxValue<int>(prefix.length(), longest_match);
		};
	}
	return longest_match;
}

string BaseKeyValueSecret::ToString(bool redact) {
	string result;

	for (auto it = secret_map.begin(); it != secret_map.end(); it++) {
		result.append(";");
		result.append(it->first);
		result.append("=");
		if (redact && redact_keys.find(it->first) != redact_keys.end()) {
			result.append("redacted");
		} else {
			result.append(it->second);
		}
	}

	return result;
}

void BaseKeyValueSecret::Serialize(Serializer &serializer) const {
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
};

} // namespace duckdb
