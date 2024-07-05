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
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

class UnionByName {
public:
	static void CombineUnionTypes(const vector<string> &new_names, const vector<LogicalType> &new_types,
	                              vector<LogicalType> &union_col_types, vector<string> &union_col_names,
	                              case_insensitive_map_t<idx_t> &union_names_map);

	//! Union all files(readers) by their col names
	template <class READER_TYPE, class OPTION_TYPE>
	static vector<typename READER_TYPE::UNION_READER_DATA>
	UnionCols(ClientContext &context, const vector<string> &files, vector<LogicalType> &union_col_types,
	          vector<string> &union_col_names, OPTION_TYPE &options) {
		vector<typename READER_TYPE::UNION_READER_DATA> union_readers;
		case_insensitive_map_t<idx_t> union_names_map;
		for (idx_t file_idx = 0; file_idx < files.size(); ++file_idx) {
			const auto &file_name = files[file_idx];
			auto reader = make_uniq<READER_TYPE>(context, file_name, options);

			auto &col_names = reader->GetNames();
			auto &sql_types = reader->GetTypes();
			CombineUnionTypes(col_names, sql_types, union_col_types, union_col_names, union_names_map);
			union_readers.push_back(READER_TYPE::StoreUnionReader(std::move(reader), file_idx));
		}
		return union_readers;
	}
};

} // namespace duckdb
