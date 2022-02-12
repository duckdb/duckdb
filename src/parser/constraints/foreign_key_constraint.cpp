#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

ForeignKeyConstraint::ForeignKeyConstraint(uint64_t fk_index, string pk_table, uint64_t pk_index)
    : Constraint(ConstraintType::FOREIGN_KEY), fk_index(fk_index), pk_table(pk_table), pk_index(pk_index) {
}
ForeignKeyConstraint::ForeignKeyConstraint(vector<string> fk_columns, string pk_table, vector<string> pk_columns)
    : Constraint(ConstraintType::FOREIGN_KEY), fk_index(DConstants::INVALID_INDEX), pk_index(DConstants::INVALID_INDEX),
      pk_table(move(pk_table)), fk_columns(move(fk_columns)), pk_columns(move(pk_columns)) {
}

string ForeignKeyConstraint::ToString() const {
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

unique_ptr<Constraint> ForeignKeyConstraint::Copy() const {
	if (fk_index == DConstants::INVALID_INDEX || pk_index == DConstants::INVALID_INDEX) {
		return make_unique<ForeignKeyConstraint>(fk_columns, pk_table, pk_columns);
	} else {
		auto result = make_unique<ForeignKeyConstraint>(fk_index, pk_table, pk_index);
		result->fk_columns = fk_columns;
		result->pk_columns = pk_columns;
		return move(result);
	}
}

void ForeignKeyConstraint::Serialize(FieldWriter &writer) const {
	writer.WriteString(pk_table);
	writer.WriteField<uint64_t>(fk_index);
	writer.WriteField<uint64_t>(pk_index);
	D_ASSERT(fk_columns.size() <= NumericLimits<uint32_t>::Maximum());
	writer.WriteList<string>(fk_columns);
	D_ASSERT(pk_columns.size() <= NumericLimits<uint32_t>::Maximum());
	writer.WriteList<string>(pk_columns);
}

unique_ptr<Constraint> ForeignKeyConstraint::Deserialize(FieldReader &source) {
	auto pk_table = source.ReadRequired<string>();
	auto fk_index = source.ReadRequired<uint64_t>();
	auto pk_index = source.ReadRequired<uint64_t>();
	auto fk_columns = source.ReadRequiredList<string>();
	auto pk_columns = source.ReadRequiredList<string>();

	if (fk_index != DConstants::INVALID_INDEX && pk_index != DConstants::INVALID_INDEX) {
		// single column parsed constraint
		auto result = make_unique<ForeignKeyConstraint>(fk_index, pk_table, pk_index);
		result->fk_columns = move(fk_columns);
		result->pk_columns = move(pk_columns);
		return move(result);
	} else {
		// column list parsed constraint
		return make_unique<ForeignKeyConstraint>(move(fk_columns), pk_table, move(pk_columns));
	}
}

} // namespace duckdb
