//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/result_format.hpp"
#include "duckdb/main/result_unit.hpp"

namespace duckdb {

class ArrowTypeExtensionData;

//! A unit holding one Arrow record batch.
class ArrowUnit : public ResultUnit {
public:
	DUCKDB_API ArrowUnit(idx_t row_count, idx_t byte_size);

public:
	ArrowArrayWrapper array;
};

//! Per-query Arrow state. The schema and the extension type map are resolved once, when the format
//! is settled, and shared by every unit. Resolving them needs the client context the properties carry
class ArrowFormatGlobalState : public ResultFormatGlobalState {
public:
	DUCKDB_API ArrowFormatGlobalState(vector<LogicalType> types, const vector<Identifier> &names,
	                                  const ClientProperties &properties);
	DUCKDB_API ~ArrowFormatGlobalState() override;

public:
	//! Owned by this state and released with it. Hand consumers a copy
	const ArrowSchema &Schema() const {
		return schema.arrow_schema;
	}
	const vector<LogicalType> &Types() const {
		return types;
	}
	const ClientProperties &Properties() const {
		return properties;
	}
	const unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> &ExtensionTypes() const {
		return extension_types;
	}

private:
	vector<LogicalType> types;
	ClientProperties properties;
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
	ArrowSchemaWrapper schema;
};

//! Turns chunks into Arrow record batches of at most batch_size rows, one appender per producer.
//! Append and Finish run concurrently on worker threads that share this object and the global
//! state, so they mutate only the local state
class ArrowFormat : public ResultFormat {
public:
	using Unit = ArrowUnit;
	using GlobalState = ArrowFormatGlobalState;
	static constexpr const char *NAME = "arrow";

public:
	DUCKDB_API explicit ArrowFormat(idx_t batch_size);

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
	//! The rows an array holds, except at a batch boundary and at a producer's end of input
	idx_t batch_size;
};

} // namespace duckdb
