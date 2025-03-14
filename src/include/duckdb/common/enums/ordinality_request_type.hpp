//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/ordinality_request_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class ordinality_request_t : uint32_t { NOT_REQUESTED = 0, REQUESTED = 1 };

struct ordinality_data_t {

	ordinality_request_t ordinality_request = ordinality_request_t::NOT_REQUESTED;
	idx_t column_id;

	void SetOrdinality(DataChunk &chunk, const idx_t &ordinality_current_idx) const {
		const idx_t ordinality = chunk.size();
		if (ordinality > 0) {
			chunk.data[column_id].Sequence(static_cast<int64_t>(ordinality_current_idx), 1, ordinality);
		}
	}

	bool operator==(const ordinality_data_t &rhs) const {
		return (this->ordinality_request == rhs.ordinality_request && this->column_id == rhs.column_id);
	}

	bool operator!=(const ordinality_data_t &rhs) const {
		return !(this == &rhs);
	}
};

} // namespace duckdb
