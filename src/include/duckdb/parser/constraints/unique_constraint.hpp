//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/constraints/unique_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/index_constraint_type.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

class UniqueConstraint : public Constraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::UNIQUE;

public:
	DUCKDB_API UniqueConstraint(const LogicalIndex index, const bool is_primary_key);
	DUCKDB_API UniqueConstraint(vector<string> columns, const bool is_primary_key);

public:
	DUCKDB_API string ToString() const override;
	DUCKDB_API unique_ptr<Constraint> Copy() const override;
	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<Constraint> Deserialize(Deserializer &deserializer);

	bool IsPrimaryKey() const {
		return is_primary_key;
	}

	bool HasIndex() const {
		return index.index != DConstants::INVALID_INDEX;
	}

	LogicalIndex GetIndex() const {
		if (!HasIndex()) {
			throw InternalException("UniqueConstraint::GetIndex called on a unique constraint without a defined index");
		}
		return index;
	}

	void SetIndex(LogicalIndex new_index) {
		D_ASSERT(new_index.index != DConstants::INVALID_INDEX);
		index = new_index;
	}

	const vector<string> &GetColumnNames() const {
		D_ASSERT(!columns.empty());
		return columns;
	}

	vector<LogicalIndex> GetLogicalIndexes(const ColumnList &columns) const {
		if (HasIndex()) {
			auto index = GetIndex();
			D_ASSERT(index.index < columns.LogicalColumnCount());
			auto &col = columns.GetColumn(index);
			return {col.Logical()};
		}

		vector<LogicalIndex> indexes;
		for (auto &col_name : GetColumnNames()) {
			D_ASSERT(columns.ColumnExists(col_name));
			auto &col = columns.GetColumn(col_name);
			D_ASSERT(!col.Generated());
			indexes.push_back(col.Logical());
		}
		return indexes;
	}

	string GetName(const string &table, const ColumnList &columns) const {
		auto type = IsPrimaryKey() ? IndexConstraintType::PRIMARY : IndexConstraintType::UNIQUE;
		auto type_name = EnumUtil::ToString(type);

		string column_names;
		for (const auto &idx : GetLogicalIndexes(columns)) {
			auto &col = columns.GetColumn(idx);
			column_names += "_" + col.Name();
		}
		return type_name + "_" + table + column_names;
	}

	vector<string> &GetColumnNamesMutable() {
		D_ASSERT(!columns.empty());
		return columns;
	}

	void SetColumnName(string name) {
		if (!columns.empty()) {
			return;
		}
		columns.push_back(std::move(name));
	}

private:
	UniqueConstraint();

#ifdef DUCKDB_API_1_0
private:
#else
public:
#endif

	//! The index of the column for which this constraint holds. Only used when the constraint relates to a single
	//! column, DConstants::INVALID_INDEX otherwise.
	LogicalIndex index;
	//! The set of columns for which this constraint holds by name. Only used when the index field is not used.
	vector<string> columns;
	//! Whether or not this is a PRIMARY KEY constraint, or a UNIQUE constraint.
	bool is_primary_key;
};

} // namespace duckdb
