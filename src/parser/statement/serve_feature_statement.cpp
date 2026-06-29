#include "duckdb/parser/statement/serve_feature_statement.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

ServeFeatureStatement::ServeFeatureStatement() : SQLStatement(StatementType::SERVE_FEATURE_STATEMENT) {
}

ServeFeatureStatement::ServeFeatureStatement(const ServeFeatureStatement &other)
    : SQLStatement(other), feature_names(other.feature_names), spine_table(other.spine_table),
      entity_mappings(other.entity_mappings), entity_column(other.entity_column), as_of_column(other.as_of_column) {
}

unique_ptr<SQLStatement> ServeFeatureStatement::Copy() const {
	return unique_ptr<ServeFeatureStatement>(new ServeFeatureStatement(*this));
}

string ServeFeatureStatement::ToString() const {
	vector<string> quoted_features;
	quoted_features.reserve(feature_names.size());
	for (auto &feature_name : feature_names) {
		quoted_features.push_back(SQLIdentifier::ToString(feature_name));
	}

	string result = "SERVE ";
	result += feature_names.size() == 1 ? "FEATURE " : "FEATURES ";
	vector<string> feature_items;
	feature_items.reserve(feature_names.size());
	for (idx_t feature_idx = 0; feature_idx < feature_names.size(); feature_idx++) {
		string item = quoted_features[feature_idx];
		if (feature_idx < entity_mappings.size() && !entity_mappings[feature_idx].empty()) {
			auto &mappings = entity_mappings[feature_idx];
			if (mappings.size() == 1 && mappings[0].feature_column.empty()) {
				item += " ENTITY " + SQLIdentifier::ToString(mappings[0].spine_column);
			} else {
				vector<string> mapping_strings;
				mapping_strings.reserve(mappings.size());
				for (auto &mapping : mappings) {
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
	if (!entity_column.empty()) {
		result += " ENTITY " + SQLIdentifier::ToString(entity_column);
	}
	if (!as_of_column.empty()) {
		result += " ASOF " + SQLIdentifier::ToString(as_of_column);
	}
	result += ";";
	return result;
}

} // namespace duckdb
