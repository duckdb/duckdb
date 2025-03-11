//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/ordinality_request_type.hpp
//
//
//===----------------------------------------------------------------------===//


#pragma once

namespace duckdb {

enum class ordinality_request : uint32_t {
  NOT_REQUESTED = 0,
  REQUESTED = 1
};

struct ordinality_data_t {

  ordinality_request ordinality_request = ordinality_request::NOT_REQUESTED;
  idx_t column_id;
  void ordinality_data_t::SetOrdinality(DataChunk &chunk, const vector<ColumnIndex> &column_ids,
	                                    idx_t &ordinality_current_idx, bool &reset) const {
	  const idx_t ordinality = chunk.size();
	  if (ordinality > 0) {
		  if (reset) {
			  ordinality_current_idx = 1;
			  reset = false;
		  }
		  chunk.data[column_id].Sequence(ordinality_current_idx, 1, ordinality);
	  }
  }
};



} // namespace duckdb

