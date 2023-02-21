#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

CommonTableExpressionInfo::CommonTableExpressionInfo() {
}

CommonTableExpressionInfo::~CommonTableExpressionInfo() {
}

CommonTableExpressionInfo::CommonTableExpressionInfo(const CommonTableExpressionInfo &) = delete;

CommonTableExpressionInfo::CommonTableExpressionInfo(CommonTableExpressionInfo &&) = default;

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
