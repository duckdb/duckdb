#include "duckdb/parser/constraints/unique_constraint.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

UniqueConstraint::UniqueConstraint() : Constraint(ConstraintType::UNIQUE), index(DConstants::INVALID_INDEX) {
}

UniqueConstraint::UniqueConstraint(LogicalIndex index, bool is_primary_key)
    : Constraint(ConstraintType::UNIQUE), index(index), is_primary_key(is_primary_key) {
}
UniqueConstraint::UniqueConstraint(vector<string> columns, bool is_primary_key)
    : Constraint(ConstraintType::UNIQUE), index(DConstants::INVALID_INDEX), columns(std::move(columns)),
      is_primary_key(is_primary_key) {
}

string UniqueConstraint::ToString() const {
	string base = is_primary_key ? "PRIMARY KEY(" : "UNIQUE(";
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			base += ", ";
		}
		base += KeywordHelper::WriteOptionallyQuoted(columns[i]);
	}
	return base + ")";
}

unique_ptr<Constraint> UniqueConstraint::Copy() const {
	if (!HasIndex()) {
		return make_uniq<UniqueConstraint>(columns, is_primary_key);
	} else {
		auto result = make_uniq<UniqueConstraint>(index, is_primary_key);
		if (!columns.empty()) {
			result->columns.push_back(columns[0]);
		}
		return std::move(result);
	}
}

} // namespace duckdb
