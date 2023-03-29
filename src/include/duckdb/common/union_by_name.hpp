//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/union_by_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class UnionByName {
public:
	static void CombineUnionTypes(const vector<string> &new_names, const vector<LogicalType> &new_types,
	                              vector<LogicalType> &union_col_types, vector<string> &union_col_names,
	                              case_insensitive_map_t<idx_t> &union_names_map);

	//! Union all files(readers) by their col names
	template <class READER_TYPE, class OPTION_TYPE>
	static vector<unique_ptr<READER_TYPE>>
	UnionCols(ClientContext &context, const vector<string> &files, vector<LogicalType> &union_col_types,
	          vector<string> &union_col_names, case_insensitive_map_t<idx_t> &union_names_map, OPTION_TYPE &options) {
		vector<unique_ptr<READER_TYPE>> union_readers;

		for (idx_t file_idx = 0; file_idx < files.size(); ++file_idx) {
			const auto file_name = files[file_idx];
			auto reader = make_unique<READER_TYPE>(context, file_name, options);

			auto &col_names = reader->GetNames();
			auto &sql_types = reader->GetTypes();
			CombineUnionTypes(col_names, sql_types, union_col_types, union_col_names, union_names_map);
			union_readers.push_back(std::move(reader));
		}
		return union_readers;
	}

	//! Create information for reader's col mapping to union cols
	template <class READER_TYPE>
	static void CreateUnionMap(vector<unique_ptr<READER_TYPE>> &union_readers, vector<LogicalType> &union_col_types,
	                           vector<string> &union_col_names, case_insensitive_map_t<idx_t> &union_names_map) {
		for (auto &reader : union_readers) {
			auto &col_names = reader->GetNames();
			vector<bool> union_null_cols(union_col_names.size(), true);

			for (idx_t col = 0; col < col_names.size(); ++col) {
				idx_t union_idx = union_names_map[col_names[col]];
				union_null_cols[union_idx] = false;
			}

			reader->reader_data.union_col_types = union_col_types;
			reader->reader_data.union_null_cols = std::move(union_null_cols);
		}
	}
};

} // namespace duckdb
