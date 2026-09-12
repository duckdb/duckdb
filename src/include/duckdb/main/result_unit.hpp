//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/result_unit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {

//! A piece of query output: what the buffer holds and what the consumer pops. Its concrete type is
//! fixed by the format that produced it, which the consumer checks by name before casting
class ResultUnit {
public:
	DUCKDB_API ResultUnit(idx_t row_count, idx_t byte_size);
	DUCKDB_API virtual ~ResultUnit();

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	//! The rows in this unit
	const idx_t row_count;
	//! The bytes this unit counts against the buffer's cap
	const idx_t byte_size;

private:
	ResultUnit(const ResultUnit &) = delete;
	ResultUnit &operator=(const ResultUnit &) = delete;
};

//! A unit holding a single buffered chunk.
class ChunkUnit : public ResultUnit {
public:
	DUCKDB_API explicit ChunkUnit(unique_ptr<DataChunk> chunk);

public:
	unique_ptr<DataChunk> chunk;
};

//! The retained storage of a result in a format other than chunks: its units, in consumption order.
//! Format-specific per-query data lives in the format's global state, never here
class ResultUnitCollection {
public:
	DUCKDB_API ResultUnitCollection();
	DUCKDB_API explicit ResultUnitCollection(vector<unique_ptr<ResultUnit>> units);
	DUCKDB_API ~ResultUnitCollection();

public:
	//! The rows the collection was built with. Fetching does not change it, the way scanning a
	//! ColumnDataCollection does not change its Count
	idx_t Count() const {
		return total_rows;
	}
	//! The units the collection was built with, fetched or not
	idx_t UnitCount() const {
		return total_units;
	}
	//! Moves the next unit out, or null once every unit has been fetched
	DUCKDB_API unique_ptr<ResultUnit> Fetch();
	//! The units not yet fetched, in consumption order
	const deque<unique_ptr<ResultUnit>> &Units() const {
		return units;
	}

private:
	deque<unique_ptr<ResultUnit>> units;
	const idx_t total_rows = 0;
	const idx_t total_units = 0;

private:
	ResultUnitCollection(const ResultUnitCollection &) = delete;
	ResultUnitCollection &operator=(const ResultUnitCollection &) = delete;
};

} // namespace duckdb
