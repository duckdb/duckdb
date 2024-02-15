//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/ordinality_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/types/data_chunk.hpp"
namespace duckdb {
struct OrdinalityData {
	bool with_ordinality = false;
	idx_t column_id;
	idx_t idx = 1;
	bool reset = false;

	void SetOrdinality(DataChunk &chunk, const vector<column_t> &column_ids) {
		const idx_t ordinality = chunk.size();
		if (ordinality > 0) {
			if (reset) {
				idx = 1;
				reset = false;
			}
			D_ASSERT(chunk.data[column_id].GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
			D_ASSERT(chunk.data[column_id].GetType().id() == duckdb::LogicalType::BIGINT);
			chunk.data[column_id].Sequence(idx, 1, ordinality);
		}
	}
};
} // namespace duckdb
