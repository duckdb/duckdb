//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/result_unit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"

#include <cstring>

namespace duckdb {

//! A piece of query output: what the buffer holds and what the consumer pops.
//! Every subclass declares a TAG naming its type, which TypeTag returns
class ResultUnit {
public:
	DUCKDB_API ResultUnit(idx_t row_count, idx_t byte_size);
	DUCKDB_API virtual ~ResultUnit();

public:
	//! The declared name of the concrete type, used in place of RTTI
	virtual const char *TypeTag() const = 0;

	//! Compared by content, because a loadable extension holds its own copy of every TAG
	template <class TARGET>
	bool Is() const {
		auto tag = TypeTag();
		return tag == TARGET::TAG || std::strcmp(tag, TARGET::TAG) == 0;
	}

	template <class TARGET>
	TARGET &Cast() {
		if (!Is<TARGET>()) {
			throw InternalException("Failed to cast result unit of type \"%s\" to \"%s\"", TypeTag(), TARGET::TAG);
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (!Is<TARGET>()) {
			throw InternalException("Failed to cast result unit of type \"%s\" to \"%s\"", TypeTag(), TARGET::TAG);
		}
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
	static constexpr const char *TAG = "chunk";

public:
	DUCKDB_API explicit ChunkUnit(unique_ptr<DataChunk> chunk);

public:
	DUCKDB_API const char *TypeTag() const override;

public:
	unique_ptr<DataChunk> chunk;
};

} // namespace duckdb
