#include "duckdb/parser/statement/serve_feature_statement.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

ServeFeatureStatement::ServeFeatureStatement() : SQLStatement(StatementType::SERVE_FEATURE_STATEMENT) {
}

ServeFeatureStatement::ServeFeatureStatement(const ServeFeatureStatement &other)
    : SQLStatement(other), feature_names(other.feature_names), spine_table(other.spine_table),
      entity_column(other.entity_column), as_of_column(other.as_of_column) {
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
	result += StringUtil::Join(quoted_features, ", ");
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
