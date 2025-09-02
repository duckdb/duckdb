#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

ForeignKeyConstraint::ForeignKeyConstraint() : Constraint(ConstraintType::FOREIGN_KEY) {
}

ForeignKeyConstraint::ForeignKeyConstraint(vector<string> pk_columns, vector<string> fk_columns, ForeignKeyInfo info)
    : Constraint(ConstraintType::FOREIGN_KEY), pk_columns(std::move(pk_columns)), fk_columns(std::move(fk_columns)),
      info(std::move(info)) {
}

string ForeignKeyConstraint::ToString() const {
	if (info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
		string base = "FOREIGN KEY (";

		for (idx_t i = 0; i < fk_columns.size(); i++) {
			if (i > 0) {
				base += ", ";
			}
			base += KeywordHelper::WriteOptionallyQuoted(fk_columns[i]);
		}
		base += ") REFERENCES ";
		if (!info.schema.empty()) {
			base += info.schema;
			base += ".";
		}
		base += info.table;
		if (!pk_columns.empty()) {
			base += "(";

			for (idx_t i = 0; i < pk_columns.size(); i++) {
				if (i > 0) {
					base += ", ";
				}
				base += KeywordHelper::WriteOptionallyQuoted(pk_columns[i]);
			}
			base += ")";
		}

		return base;
	}

	return "";
}

unique_ptr<Constraint> ForeignKeyConstraint::Copy() const {
	return make_uniq<ForeignKeyConstraint>(pk_columns, fk_columns, info);
}

} // namespace duckdb
