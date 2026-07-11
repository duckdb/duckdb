#include "duckdb/parser/statement/refresh_feature_statement.hpp"

#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

RefreshFeatureStatement::RefreshFeatureStatement() : SQLStatement(StatementType::REFRESH_FEATURE_STATEMENT) {
}

RefreshFeatureStatement::RefreshFeatureStatement(const RefreshFeatureStatement &other)
    : SQLStatement(other), feature_name(other.feature_name), at_timestamp(other.at_timestamp) {
}

unique_ptr<SQLStatement> RefreshFeatureStatement::Copy() const {
	return unique_ptr<RefreshFeatureStatement>(new RefreshFeatureStatement(*this));
}

string RefreshFeatureStatement::ToString() const {
	string result = "REFRESH FEATURE " + SQLIdentifier::ToString(feature_name);
	if (!at_timestamp.empty()) {
		result += " AT '" + at_timestamp + "'";
	}
	return result + ";";
}

} // namespace duckdb
