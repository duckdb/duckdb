#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

ForeignKeyConstraint::ForeignKeyConstraint(string pk_table, vector<string> pk_columns, vector<string> fk_columns,
                                           bool is_fk_table)
    : Constraint(ConstraintType::FOREIGN_KEY), pk_table(move(pk_table)), pk_columns(move(pk_columns)),
      fk_columns(move(fk_columns)), is_fk_table(is_fk_table) {
}

string ForeignKeyConstraint::ToString() const {
	if (is_fk_table) {
		string base = "FOREIGN KEY (";

		for (idx_t i = 0; i < fk_columns.size(); i++) {
			if (i > 0) {
				base += ", ";
			}
			base += KeywordHelper::WriteOptionallyQuoted(fk_columns[i]);
		}
		base += ") REFERENCES (";

		for (idx_t i = 0; i < pk_columns.size(); i++) {
			if (i > 0) {
				base += ", ";
			}
			base += KeywordHelper::WriteOptionallyQuoted(pk_columns[i]);
		}
		base += ")";

		return base;
	}

	return "";
}

unique_ptr<Constraint> ForeignKeyConstraint::Copy() const {
	return make_unique<ForeignKeyConstraint>(pk_table, pk_columns, fk_columns, is_fk_table);
}

void ForeignKeyConstraint::Serialize(FieldWriter &writer) const {
	writer.WriteString(pk_table);
	D_ASSERT(pk_columns.size() <= NumericLimits<uint32_t>::Maximum());
	writer.WriteList<string>(pk_columns);
	D_ASSERT(fk_columns.size() <= NumericLimits<uint32_t>::Maximum());
	writer.WriteList<string>(fk_columns);
	writer.WriteField<bool>(is_fk_table);
}

unique_ptr<Constraint> ForeignKeyConstraint::Deserialize(FieldReader &source) {
	auto pk_table = source.ReadRequired<string>();
	auto pk_columns = source.ReadRequiredList<string>();
	auto fk_columns = source.ReadRequiredList<string>();
	auto is_fk_table = source.ReadRequired<bool>();

	// column list parsed constraint
	return make_unique<ForeignKeyConstraint>(pk_table, move(pk_columns), move(fk_columns), is_fk_table);
}

} // namespace duckdb
