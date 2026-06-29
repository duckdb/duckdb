#include "duckdb/parser/statement/serve_feature_statement.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

ServeFeatureStatement::ServeFeatureStatement() : SQLStatement(StatementType::SERVE_FEATURE_STATEMENT) {
}

ServeFeatureStatement::ServeFeatureStatement(const ServeFeatureStatement &other)
    : SQLStatement(other), features(other.features), spine_table(other.spine_table),
      spine_entity_override(other.spine_entity_override), spine_asof_column(other.spine_asof_column) {
}

unique_ptr<SQLStatement> ServeFeatureStatement::Copy() const {
	return unique_ptr<ServeFeatureStatement>(new ServeFeatureStatement(*this));
}

string ServeFeatureStatement::ToString() const {
	string result = "SERVE ";
	result += features.size() == 1 ? "FEATURE " : "FEATURES ";
	vector<string> feature_items;
	feature_items.reserve(features.size());
	for (auto &feature : features) {
		string item = SQLIdentifier::ToString(feature.feature_name);
		if (!feature.entity_mappings.empty()) {
			if (feature.entity_mappings.size() == 1 && feature.entity_mappings[0].feature_column.empty()) {
				item += " ENTITY " + SQLIdentifier::ToString(feature.entity_mappings[0].spine_column);
			} else {
				vector<string> mapping_strings;
				mapping_strings.reserve(feature.entity_mappings.size());
				for (auto &mapping : feature.entity_mappings) {
					string mapping_string = SQLIdentifier::ToString(mapping.feature_column);
					if (mapping.spine_column != mapping.feature_column) {
						mapping_string += " = " + SQLIdentifier::ToString(mapping.spine_column);
					}
					mapping_strings.push_back(mapping_string);
				}
				item += " ENTITY (" + StringUtil::Join(mapping_strings, ", ") + ")";
			}
		}
		feature_items.push_back(std::move(item));
	}
	result += StringUtil::Join(feature_items, ", ");
	result += " FOR " + SQLIdentifier::ToString(spine_table);
	if (!spine_entity_override.empty()) {
		result += " ENTITY " + SQLIdentifier::ToString(spine_entity_override);
	}
	if (!spine_asof_column.empty()) {
		result += " ASOF " + SQLIdentifier::ToString(spine_asof_column);
	}
	result += ";";
	return result;
}

} // namespace duckdb
