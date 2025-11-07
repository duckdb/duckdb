#include "duckdb/parser/statement/cluster_statement.hpp"

namespace duckdb {

ClusterStatement::ClusterStatement() : SQLStatement(StatementType::CLUSTER_STATEMENT) {
}

ClusterStatement::ClusterStatement(const ClusterStatement &other) : SQLStatement(other) {
	target = other.target->Copy();
	for (auto &order : other.modifiers) {
		modifiers.emplace_back(order.type, order.null_order, order.expression->Copy());
	}
}

string ClusterStatement::ToString() const {
	string result;
	result += "CLUSTER " + target->ToString() + " ORDER BY ";
	for (idx_t i = 0; i < modifiers.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += modifiers[i].ToString();
	}
	return result;
}

unique_ptr<SQLStatement> ClusterStatement::Copy() const {
	return unique_ptr<ClusterStatement>(new ClusterStatement(*this));
}

} // namespace duckdb
