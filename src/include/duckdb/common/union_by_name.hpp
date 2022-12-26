//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/union_names.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include<vector>
#include<string>
#include "duckdb/common/types.hpp"

using std::string;
using std::vector;

namespace duckdb {

template <class READER_TYPE, class OPTION_TYPE>
class UnionByName {

public:
	//! Union all  files by their col names
	DUCKDB_API static vector<unique_ptr<READER_TYPE>> UnionCols(ClientContext &context, const vector<string> &files, 
									vector<LogicalType> &union_col_types, vector<string> &union_col_names,
									case_insensitive_map_t<idx_t> &union_names_map, OPTION_TYPE options) {
		idx_t union_names_index = 0;
		vector<unique_ptr<READER_TYPE>> union_readers;

		for (idx_t file_idx = 0; file_idx < files.size(); ++file_idx) {
			const auto file_name = files[file_idx];
			auto reader = make_unique<READER_TYPE>(context, file_name, options); 

			auto &col_names = reader->names;
			auto &sql_types = reader->return_types;
			D_ASSERT(col_names.size() == sql_types.size());

			for (idx_t col = 0; col < col_names.size(); ++col) {
				auto union_find = union_names_map.find(col_names[col]);

				if (union_find != union_names_map.end()) {
					// given same name , union_col's type must compatible with col's type
					LogicalType compatible_type;
					compatible_type = LogicalType::MaxLogicalType(union_col_types[union_find->second], sql_types[col]);
					union_col_types[union_find->second] = compatible_type;
				} else {
					union_names_map[col_names[col]] = union_names_index;
					union_names_index++;

					union_col_names.emplace_back(col_names[col]);
					union_col_types.emplace_back(sql_types[col]);
				}
			}
			union_readers.push_back(move(reader));
		}
		return union_readers;
	}
};
} // namespace duckdb
