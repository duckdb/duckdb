//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/result_unit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/result_unit_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {

//! A piece of query output: what the buffer holds and what the consumer pops.
class ResultUnit {
public:
	DUCKDB_API explicit ResultUnit(ResultUnitType type);
	DUCKDB_API virtual ~ResultUnit();

public:
	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != type) {
			throw InternalException("Failed to cast result unit to type - result unit type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (TARGET::TYPE != type) {
			throw InternalException("Failed to cast result unit to type - result unit type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	//! What this unit holds
	ResultUnitType type;
	//! The rows in this unit
	idx_t row_count = 0;
	//! The bytes this unit counts against the buffer's cap
	idx_t byte_size = 0;
	//! INVALID_INDEX unless the sink is batch ordered
	idx_t batch_index = DConstants::INVALID_INDEX;

private:
	ResultUnit(const ResultUnit &) = delete;
	ResultUnit &operator=(const ResultUnit &) = delete;
};

//! A unit holding a single buffered chunk.
class ChunkUnit : public ResultUnit {
public:
	static constexpr const ResultUnitType TYPE = ResultUnitType::CHUNK;

public:
	DUCKDB_API explicit ChunkUnit(unique_ptr<DataChunk> chunk);

public:
	unique_ptr<DataChunk> chunk;
};

} // namespace duckdb
