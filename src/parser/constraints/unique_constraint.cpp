#include "duckdb/parser/constraints/unique_constraint.hpp"

#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

UniqueConstraint::UniqueConstraint() : Constraint(ConstraintType::UNIQUE), index(DConstants::INVALID_INDEX) {
}

UniqueConstraint::UniqueConstraint(const LogicalIndex index, const bool is_primary_key)
    : Constraint(ConstraintType::UNIQUE), index(index), is_primary_key(is_primary_key) {
}
UniqueConstraint::UniqueConstraint(const LogicalIndex index, string column_name_p, const bool is_primary_key)
    : UniqueConstraint(index, is_primary_key) {
	columns.push_back(std::move(column_name_p));
}

UniqueConstraint::UniqueConstraint(vector<string> columns, const bool is_primary_key)
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
	}

	auto result = make_uniq<UniqueConstraint>(index, columns.empty() ? string() : columns[0], is_primary_key);
	return std::move(result);
}

bool UniqueConstraint::IsPrimaryKey() const {
	return is_primary_key;
}

bool UniqueConstraint::HasIndex() const {
	return index.index != DConstants::INVALID_INDEX;
}

LogicalIndex UniqueConstraint::GetIndex() const {
	if (!HasIndex()) {
		throw InternalException("UniqueConstraint::GetIndex called on a unique constraint without an index");
	}
	return index;
}

void UniqueConstraint::SetIndex(const LogicalIndex new_index) {
	D_ASSERT(new_index.index != DConstants::INVALID_INDEX);
	index = new_index;
}

const vector<string> &UniqueConstraint::GetColumnNames() const {
	D_ASSERT(!columns.empty());
	return columns;
}

vector<string> &UniqueConstraint::GetColumnNamesMutable() {
	D_ASSERT(!columns.empty());
	return columns;
}

vector<LogicalIndex> UniqueConstraint::GetLogicalIndexes(const ColumnList &column_list) const {
	if (HasIndex()) {
		return {GetIndex()};
	}

	vector<LogicalIndex> indexes;
	for (auto &col_name : GetColumnNames()) {
		D_ASSERT(column_list.ColumnExists(col_name));
		auto &col = column_list.GetColumn(col_name);
		D_ASSERT(!col.Generated());
		indexes.push_back(col.Logical());
	}
	return indexes;
}

string UniqueConstraint::GetName(const string &table_name) const {
	auto type = IsPrimaryKey() ? IndexConstraintType::PRIMARY : IndexConstraintType::UNIQUE;
	auto type_name = EnumUtil::ToString(type);

	string name;
	for (const auto &column_name : GetColumnNames()) {
		name += "_" + column_name;
	}
	return type_name + "_" + table_name + name;
}

} // namespace duckdb
