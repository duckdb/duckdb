#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

NotNullConstraint::NotNullConstraint(LogicalIndex index, string column_name)
    : Constraint(ConstraintType::NOT_NULL), index(index), column_name(column_name) {
}

NotNullConstraint::~NotNullConstraint() {
}

string NotNullConstraint::ToString() const {
	if (!index.IsValid()) {
		D_ASSERT(!column_name.empty());
		return StringUtil::Format("%s NOT NULL", column_name);
	}
	return "NOT NULL";
}

unique_ptr<Constraint> NotNullConstraint::Copy() const {
	if (!index.IsValid()) {
		D_ASSERT(!column_name.empty());
		return make_uniq<NotNullConstraint>(index, column_name);
	}
	return make_uniq<NotNullConstraint>(index);
}

} // namespace duckdb
