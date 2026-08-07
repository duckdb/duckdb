#include "duckdb/parser/parsed_data/external_resource_options.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

unique_ptr<ExternalResourceOptions> ExternalResourceOptions::Copy() const {
	auto result = make_uniq<ExternalResourceOptions>();
	for (auto &entry : parsed_params) {
		result->parsed_params[entry.first] = entry.second->Copy();
	}
	result->alias = alias;
	result->provider = provider;
	result->params = params;
	result->reference_name = reference_name;
	return result;
}

string ExternalResourceOptions::ToString() const {
	// Reference form: a bare identifier naming an already-registered resource.
	if (!reference_name.empty()) {
		return "EXTERNAL RESOURCE " + SQLIdentifier(reference_name);
	}
	// Create form: NEW TEMPORARY EXTERNAL RESOURCE '<type>' [(create opts)].
	string result = "NEW TEMPORARY EXTERNAL RESOURCE " + SQLString(provider);
	vector<string> stringified;
	for (auto &entry : parsed_params) {
		stringified.push_back(StringUtil::Format("%s %s", entry.first, entry.second->ToString()));
	}
	for (auto &entry : params) {
		stringified.push_back(StringUtil::Format("%s %s", entry.first, entry.second.ToSQLString()));
	}
	if (!stringified.empty()) {
		result += " (" + StringUtil::Join(stringified, ", ") + ")";
	}
	return result;
}

void ExternalResourceOptions::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<Identifier>(100, "alias", alias);
	serializer.WritePropertyWithDefault<string>(101, "provider", provider);
	serializer.WritePropertyWithDefault<unordered_map<string, Value>>(102, "params", params);
	serializer.WritePropertyWithDefault<string>(103, "reference_name", reference_name);
}

unique_ptr<ExternalResourceOptions> ExternalResourceOptions::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<ExternalResourceOptions>();
	deserializer.ReadPropertyWithDefault<Identifier>(100, "alias", result->alias);
	deserializer.ReadPropertyWithDefault<string>(101, "provider", result->provider);
	deserializer.ReadPropertyWithDefault<unordered_map<string, Value>>(102, "params", result->params);
	deserializer.ReadPropertyWithDefault<string>(103, "reference_name", result->reference_name);
	return result;
}

} // namespace duckdb
