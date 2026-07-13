#include "duckdb/parser/tableref/serve_feature_ref.hpp"

#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

static string EntityMappingToString(const vector<FeatureServeEntityMapping> &mappings) {
	// A single mapping with no feature column is the shorthand form: ENTITY <spine_column>.
	if (mappings.size() == 1 && mappings[0].feature_column.empty()) {
		return " ENTITY " + SQLIdentifier(mappings[0].spine_column);
	}
	string result = " ENTITY (";
	for (idx_t i = 0; i < mappings.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += SQLIdentifier(mappings[i].feature_column) + " = " + SQLIdentifier(mappings[i].spine_column);
	}
	result += ")";
	return result;
}

string ServeFeatureRef::ToString() const {
	string result = features.size() > 1 ? "SERVE FEATURES " : "SERVE FEATURE ";
	for (idx_t i = 0; i < features.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += SQLIdentifier(features[i].feature_name);
		if (!features[i].entity_mappings.empty()) {
			result += EntityMappingToString(features[i].entity_mappings);
		}
	}
	result += " FOR " + SQLIdentifier(spine_table);
	if (!spine_entity_override.empty()) {
		result += " ENTITY " + SQLIdentifier(spine_entity_override);
	}
	if (!spine_asof_column.empty()) {
		result += " ASOF " + SQLIdentifier(spine_asof_column);
	}
	return BaseToString(result, column_name_alias);
}

bool ServeFeatureRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ServeFeatureRef>();
	return features == other.features && spine_table == other.spine_table &&
	       spine_entity_override == other.spine_entity_override && spine_asof_column == other.spine_asof_column;
}

unique_ptr<TableRef> ServeFeatureRef::Copy() {
	auto copy = make_uniq<ServeFeatureRef>();
	copy->features = features;
	copy->spine_table = spine_table;
	copy->spine_entity_override = spine_entity_override;
	copy->spine_asof_column = spine_asof_column;
	CopyProperties(*copy);
	return std::move(copy);
}

} // namespace duckdb
