//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {

struct RowGroupExpressionFilter {
	RowGroupExpressionFilter();
	RowGroupExpressionFilter(vector<ProjectionIndex> column_indexes, unique_ptr<Expression> expression);

	vector<ProjectionIndex> column_indexes;
	unique_ptr<Expression> expression;

	bool Equals(const RowGroupExpressionFilter &other) const;
	bool operator==(const RowGroupExpressionFilter &other) const {
		return Equals(other);
	}
	RowGroupExpressionFilter Copy() const;

	void Serialize(Serializer &serializer) const;
	static RowGroupExpressionFilter Deserialize(Deserializer &deserializer);
};

//! The regular filters in here only need a single column to be evaluated.
//! Composite filters can be stored separately for row-group pruning.
class TableFilterSet {
public:
	void PushFilter(ProjectionIndex col_idx, unique_ptr<TableFilter> filter);
	void PushRowGroupFilter(RowGroupExpressionFilter filter);
	bool HasFilters() const;
	idx_t FilterCount() const;
	bool HasRowGroupFilters() const;
	bool HasFilter(ProjectionIndex col_idx) const;
	TableFilter &GetFilterByColumnIndexMutable(ProjectionIndex col_idx);
	optional_ptr<TableFilter> TryGetFilterByColumnIndexMutable(ProjectionIndex col_idx);
	const TableFilter &GetFilterByColumnIndex(ProjectionIndex col_idx) const;
	optional_ptr<const TableFilter> TryGetFilterByColumnIndex(ProjectionIndex col_idx) const;
	void SetFilterByColumnIndex(ProjectionIndex col_idx, unique_ptr<TableFilter> filter);
	void RemoveFilterByColumnIndex(ProjectionIndex col_idx);
	void ClearFilters();
	const vector<RowGroupExpressionFilter> &GetRowGroupFilters() const;

	bool Equals(TableFilterSet &other);
	static bool Equals(TableFilterSet *left, TableFilterSet *right);

	unique_ptr<TableFilterSet> Copy() const;

	void Serialize(Serializer &serializer) const;
	static TableFilterSet Deserialize(Deserializer &deserializer);

	map<ProjectionIndex, unique_ptr<TableFilter>> GetTableFiltersForSerialization(Serializer &serializer) const;
	map<ProjectionIndex, unique_ptr<TableFilter>> &GetTableFiltersForDeserialization(Deserializer &deserializer);

public:
	class TableFilterIteratorEntry {
	public:
		explicit TableFilterIteratorEntry(map<ProjectionIndex, unique_ptr<TableFilter>>::iterator);

		ProjectionIndex GetIndex() const;
		TableFilter &Filter();
		const TableFilter &Filter() const;
		unique_ptr<TableFilter> TakeFilter();

	public:
		map<ProjectionIndex, unique_ptr<TableFilter>>::iterator iterator;
	};

	class ConstTableFilterIteratorEntry {
	public:
		explicit ConstTableFilterIteratorEntry(map<ProjectionIndex, unique_ptr<TableFilter>>::const_iterator);

		ProjectionIndex GetIndex() const;
		const TableFilter &Filter() const;

	public:
		map<ProjectionIndex, unique_ptr<TableFilter>>::const_iterator iterator;
	};

	// iterator
	template <class T>
	class TableFilterIterator {
	public:
		explicit TableFilterIterator(T entry_p) : entry(std::move(entry_p)) {
		}

	public:
		TableFilterIterator &operator++() {
			++entry.iterator;
			return *this;
		}
		bool operator!=(const TableFilterIterator &other) const {
			return entry.iterator != other.entry.iterator;
		}
		T &operator*() {
			return entry;
		}
		const T &operator*() const {
			return entry;
		}

	private:
		T entry;
	};

	TableFilterIterator<TableFilterIteratorEntry> begin() { // NOLINT: match stl API
		return TableFilterIterator<TableFilterIteratorEntry>(TableFilterIteratorEntry(filters.begin()));
	}
	TableFilterIterator<TableFilterIteratorEntry> end() { // NOLINT: match stl API
		return TableFilterIterator<TableFilterIteratorEntry>(TableFilterIteratorEntry(filters.end()));
	}
	TableFilterIterator<ConstTableFilterIteratorEntry> begin() const { // NOLINT: match stl API
		return TableFilterIterator<ConstTableFilterIteratorEntry>(ConstTableFilterIteratorEntry(filters.begin()));
	}
	TableFilterIterator<ConstTableFilterIteratorEntry> end() const { // NOLINT: match stl API
		return TableFilterIterator<ConstTableFilterIteratorEntry>(ConstTableFilterIteratorEntry(filters.end()));
	}

private:
	map<ProjectionIndex, unique_ptr<TableFilter>> filters;
	vector<RowGroupExpressionFilter> row_group_filters;
};

class DynamicTableFilterSet {
public:
	void ClearFilters(const PhysicalOperator &op);
	void PushFilter(const PhysicalOperator &op, ProjectionIndex column_index, unique_ptr<TableFilter> filter);

	bool HasFilters() const;
	unique_ptr<TableFilterSet> GetFinalTableFilters(const PhysicalTableScan &scan,
	                                                optional_ptr<TableFilterSet> existing_filters) const;

private:
	mutable mutex lock;
	reference_map_t<const PhysicalOperator, unique_ptr<TableFilterSet>> filters;
};

} // namespace duckdb
