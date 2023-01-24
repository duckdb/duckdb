#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

CommonTableExpressionInfo::CommonTableExpressionInfo() {
}

CommonTableExpressionInfo::~CommonTableExpressionInfo() {
}

bool CommonTableExpressionInfo::Equals(const CommonTableExpressionInfo &other) const {
	if (aliases != other.aliases) {
		return false;
	}
	if (!query && !other.query) {
		return true;
	}
	if (!query || !other.query) {
		return false;
	}
	return query->Equals(other.query.get());
}

} // namespace duckdb
