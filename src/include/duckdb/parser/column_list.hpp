//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/column_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

//! A set of column definitions
class ColumnList {
public:
	class ColumnListIterator;

public:
	ColumnList(bool allow_duplicate_names = false);

	void AddColumn(ColumnDefinition column);
	void Finalize();

	const ColumnDefinition &GetColumn(LogicalIndex index) const;
	const ColumnDefinition &GetColumn(PhysicalIndex index) const;
	const ColumnDefinition &GetColumn(const string &name) const;
	ColumnDefinition &GetColumnMutable(LogicalIndex index);
	ColumnDefinition &GetColumnMutable(PhysicalIndex index);
	ColumnDefinition &GetColumnMutable(const string &name);

	bool ColumnExists(const string &name) const;

	LogicalIndex GetColumnIndex(string &column_name) const;
	PhysicalIndex LogicalToPhysical(LogicalIndex index) const;

	idx_t LogicalColumnCount() const {
		return columns.size();
	}
	idx_t PhysicalColumnCount() const {
		return physical_columns.size();
	}
	bool empty() const {
		return columns.empty();
	}

	ColumnList Copy() const;
	void Serialize(FieldWriter &writer) const;
	static ColumnList Deserialize(FieldReader &reader);

	ColumnListIterator Logical() const;
	ColumnListIterator Physical() const;

	void SetAllowDuplicates(bool allow_duplicates) {
		allow_duplicate_names = allow_duplicates;
	}

private:
	vector<ColumnDefinition> columns;
	//! A map of column name to column index
	case_insensitive_map_t<column_t> name_map;
	//! The set of physical columns
	vector<idx_t> physical_columns;
	//! Allow duplicate names or not
	bool allow_duplicate_names;

private:
	void AddToNameMap(ColumnDefinition &column);

public:
	// logical iterator
	class ColumnListIterator {
	public:
		DUCKDB_API ColumnListIterator(const ColumnList &list, bool physical) : list(list), physical(physical) {
		}

	private:
		const ColumnList &list;
		bool physical;

	private:
		class ColumnLogicalIteratorInternal {
		public:
			DUCKDB_API ColumnLogicalIteratorInternal(const ColumnList &list, bool physical, idx_t pos, idx_t end)
			    : list(list), physical(physical), pos(pos), end(end) {
			}

			const ColumnList &list;
			bool physical;
			idx_t pos;
			idx_t end;

		public:
			DUCKDB_API ColumnLogicalIteratorInternal &operator++() {
				pos++;
				return *this;
			}
			DUCKDB_API bool operator!=(const ColumnLogicalIteratorInternal &other) const {
				return pos != other.pos || end != other.end || &list != &other.list;
			}
			DUCKDB_API const ColumnDefinition &operator*() const {
				if (physical) {
					return list.GetColumn(PhysicalIndex(pos));
				} else {
					return list.GetColumn(LogicalIndex(pos));
				}
			}
		};

	public:
		idx_t Size() {
			return physical ? list.PhysicalColumnCount() : list.LogicalColumnCount();
		}

		DUCKDB_API ColumnLogicalIteratorInternal begin() {
			return ColumnLogicalIteratorInternal(list, physical, 0, Size());
		}
		DUCKDB_API ColumnLogicalIteratorInternal end() {
			return ColumnLogicalIteratorInternal(list, physical, Size(), Size());
		}
	};
};

} // namespace duckdb
