//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/result_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/result_ordering.hpp"
#include "duckdb/common/enums/result_unit_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/result_unit.hpp"

namespace duckdb {

class DataChunk;

//! Per-query state of a format, built once when the format is settled.
class ResultFormatGlobalState {
public:
	DUCKDB_API virtual ~ResultFormatGlobalState();

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
};

//! Per-worker state of a format: the unit under construction.
class ResultFormatLocalState {
public:
	DUCKDB_API virtual ~ResultFormatLocalState();

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
};

//! Everything a format's global state is built from. Captured on the client thread at submission,
//! because a worker may not read settings while the query runs
struct ResultFormatContext {
	vector<LogicalType> types;
	vector<Identifier> names;
	ClientProperties client_properties;
	ResultOrdering ordering = ResultOrdering::UNORDERED;
};

//! Turns the chunks a query produces into units. Every subclass declares Unit,
//! GlobalState, a NAME and a TYPE tag. NAME is what the result's accessors and the stream
//! constructors check against the settled format, because every format outside the engine shares
//! one ResultUnitType.
//! Append, IsFull and Finish run concurrently on worker threads that share the format object and the
//! global state, so a format may mutate only its local state without synchronization
class ResultFormat {
public:
	DUCKDB_API virtual ~ResultFormat();

public:
	//! Identifies this format across every accessor and stream check
	virtual const char *Name() const = 0;
	//! Once per query, on the thread that settles the format
	virtual unique_ptr<ResultFormatGlobalState> InitGlobal(const vector<LogicalType> &types,
	                                                       const vector<Identifier> &names,
	                                                       const ClientProperties &properties,
	                                                       ResultOrdering ordering) = 0;
	//! Once per worker thread, at its first Append
	virtual unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) = 0;
	//! Convert one chunk into the unit under construction. Runs on a worker thread
	virtual void Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate, DataChunk &chunk) = 0;
	//! Whether the unit under construction reached the format's size target
	virtual bool IsFull(ResultFormatLocalState &lstate) = 0;
	//! Hand over the unit under construction with row_count and byte_size set. Null when there is none
	virtual unique_ptr<ResultUnit> Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate) = 0;

public:
	//! Whether this is the identity format
	DUCKDB_API bool IsChunk() const;
	//! The identity format, shared by every query that asks for no other: a null format anywhere
	//! means this instance
	DUCKDB_API static const shared_ptr<ResultFormat> &Chunk();
};

//! The identity format: one unit per chunk, holding the copy the buffer used to make itself.
class ChunkFormat : public ResultFormat {
public:
	using Unit = DataChunk;
	using GlobalState = ResultFormatGlobalState;
	static constexpr const char *NAME = "chunk";
	static constexpr const ResultUnitType TYPE = ResultUnitType::CHUNK;

public:
	DUCKDB_API const char *Name() const override;
	DUCKDB_API unique_ptr<ResultFormatGlobalState> InitGlobal(const vector<LogicalType> &types,
	                                                          const vector<Identifier> &names,
	                                                          const ClientProperties &properties,
	                                                          ResultOrdering ordering) override;
	DUCKDB_API unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) override;
	DUCKDB_API void Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate, DataChunk &chunk) override;
	DUCKDB_API bool IsFull(ResultFormatLocalState &lstate) override;
	DUCKDB_API unique_ptr<ResultUnit> Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate) override;
};

} // namespace duckdb
