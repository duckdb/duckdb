//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/result_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/query_result_memory_type.hpp"
#include "duckdb/common/enums/result_ordering.hpp"
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

class BatchedDataCollection;
class ClientContext;
class ColumnDataCollection;
class DataChunk;

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

//! Captured on the client thread at submission, because a worker may not read settings while the query runs
struct ResultFormatContext {
	vector<LogicalType> types;
	vector<Identifier> names;
	ClientProperties client_properties;
	ResultOrdering ordering = ResultOrdering::UNORDERED;
};

//! Subclasses declare Unit, GlobalState and NAME. Formats are identified by NAME, never by type
//! Workers call Append and Finish concurrently and share the format and global state, so only local state is mutable
class ResultFormat {
public:
	DUCKDB_API virtual ~ResultFormat();

public:
	virtual const char *Name() const = 0;
	virtual unique_ptr<ResultFormatGlobalState> InitGlobal(const vector<LogicalType> &types,
	                                                       const vector<Identifier> &names,
	                                                       const ClientProperties &properties,
	                                                       ResultOrdering ordering) = 0;
	virtual unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) = 0;
	virtual void Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate, DataChunk &chunk) = 0;
	//! flush_partial hands over the unit under construction short of its cap, so no unit spans two batch indexes
	virtual unique_ptr<ResultUnit> Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate,
	                                      bool flush_partial) = 0;

public:
	DUCKDB_API bool IsChunk() const;
	//! A null format anywhere means this instance
	DUCKDB_API static const shared_ptr<ResultFormat> &Chunk();

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

class ChunkFormat : public ResultFormat {
public:
	using Unit = DataChunk;
	using GlobalState = ResultFormatGlobalState;
	static constexpr const char *NAME = "chunk";

public:
	DUCKDB_API explicit ChunkFormat(QueryResultMemoryType memory_type = QueryResultMemoryType::IN_MEMORY);

public:
	DUCKDB_API static const shared_ptr<ResultFormat> &InMemory();
	//! Rows count against memory_limit and can spill, and the result throws once the database has closed
	DUCKDB_API static const shared_ptr<ResultFormat> &BufferManaged();

	DUCKDB_API QueryResultMemoryType MemoryType() const;
	DUCKDB_API unique_ptr<ColumnDataCollection> CreateCollection(ClientContext &context,
	                                                             const vector<LogicalType> &types) const;
	DUCKDB_API unique_ptr<BatchedDataCollection> CreateBatchedCollection(ClientContext &context,
	                                                                     vector<LogicalType> types) const;

public:
	DUCKDB_API const char *Name() const override;
	DUCKDB_API unique_ptr<ResultFormatGlobalState> InitGlobal(const vector<LogicalType> &types,
	                                                          const vector<Identifier> &names,
	                                                          const ClientProperties &properties,
	                                                          ResultOrdering ordering) override;
	DUCKDB_API unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) override;
	DUCKDB_API void Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate, DataChunk &chunk) override;
	DUCKDB_API unique_ptr<ResultUnit> Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate,
	                                         bool flush_partial) override;

private:
	QueryResultMemoryType memory_type;
};

} // namespace duckdb
