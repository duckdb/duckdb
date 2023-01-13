#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

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
		base += "(";

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
	return make_unique<ForeignKeyConstraint>(pk_columns, fk_columns, info);
}

void ForeignKeyConstraint::Serialize(FieldWriter &writer) const {
	D_ASSERT(pk_columns.size() <= NumericLimits<uint32_t>::Maximum());
	writer.WriteList<string>(pk_columns);
	D_ASSERT(fk_columns.size() <= NumericLimits<uint32_t>::Maximum());
	writer.WriteList<string>(fk_columns);
	writer.WriteField<ForeignKeyType>(info.type);
	writer.WriteString(info.schema);
	writer.WriteString(info.table);
	writer.WriteIndexList<PhysicalIndex>(info.pk_keys);
	writer.WriteIndexList<PhysicalIndex>(info.fk_keys);
}

unique_ptr<Constraint> ForeignKeyConstraint::Deserialize(FieldReader &source) {
	ForeignKeyInfo read_info;
	auto pk_columns = source.ReadRequiredList<string>();
	auto fk_columns = source.ReadRequiredList<string>();
	read_info.type = source.ReadRequired<ForeignKeyType>();
	read_info.schema = source.ReadRequired<string>();
	read_info.table = source.ReadRequired<string>();
	read_info.pk_keys = source.ReadRequiredIndexList<PhysicalIndex>();
	read_info.fk_keys = source.ReadRequiredIndexList<PhysicalIndex>();

	// column list parsed constraint
	return make_unique<ForeignKeyConstraint>(pk_columns, fk_columns, std::move(read_info));
}

} // namespace duckdb
