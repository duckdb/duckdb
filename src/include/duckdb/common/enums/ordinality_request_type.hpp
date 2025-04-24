//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/ordinality_request_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

enum class Ordinality_request_t : uint8_t { NOT_REQUESTED = 0, REQUESTED = 1 };

struct ordinality_data_t {

	Ordinality_request_t ordinality_request = Ordinality_request_t::NOT_REQUESTED;
	idx_t column_id;

	void SetOrdinality(DataChunk &chunk, const idx_t &ordinality_idx, const idx_t &ordinality) const {
		if (ordinality > 0) {
			constexpr idx_t step = 1;
			chunk.data[column_id].Sequence(static_cast<int64_t>(ordinality_idx), step, ordinality);
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
