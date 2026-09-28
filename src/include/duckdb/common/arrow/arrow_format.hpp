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
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/result_format.hpp"
#include "duckdb/main/retained_result_collection.hpp"

namespace duckdb {

class ArrowTypeExtensionData;

//! One Arrow array as a consumer receives it. Its buffers are shared: every copy holds its own view,
//! a struct tree over the same buffers whose release drops one reference, and the buffers go with the
//! last holder
class ArrowPayload {
public:
	//! Takes over the array, which the appender finalized
	DUCKDB_API explicit ArrowPayload(ArrowArray array);
	//! A payload over buffers another payload already holds
	DUCKDB_API explicit ArrowPayload(shared_ptr<ArrowArrayWrapper> owner);

public:
	DUCKDB_API unique_ptr<ArrowPayload> Copy() const;

public:
	//! This payload's view over the shared buffers, handed to a consumer with MoveTo
	ArrowArrayWrapper array;

private:
	shared_ptr<ArrowArrayWrapper> owner;
};

//! Per-query Arrow state, built at submission. The schema and the extension type map are resolved once
//! and shared by every array. Resolving them needs the client context the properties carry
class ArrowFormatGlobalState : public ResultFormatGlobalState {
public:
	DUCKDB_API explicit ArrowFormatGlobalState(const ResultFormatContext &context);
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

//! Turns chunks into Arrow arrays of at most batch_size rows, one appender per producer
class ArrowFormat : public ResultFormatBase<ArrowFormat> {
public:
	using T = ArrowPayload;
	using GlobalState = ArrowFormatGlobalState;
	static constexpr const char *NAME = "arrow";

public:
	DUCKDB_API explicit ArrowFormat(idx_t batch_size);

public:
	DUCKDB_API const char *Name() const override;
	DUCKDB_API unique_ptr<ResultFormatGlobalState> InitGlobal(const ResultFormatContext &context) override;
	DUCKDB_API unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) override;
	DUCKDB_API void AppendToUnit(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate,
	                             DataChunk &chunk) override;
	DUCKDB_API bool IsUnitFinished(ResultFormatLocalState &lstate) override;
	DUCKDB_API unique_ptr<ResultUnit> FinishUnit(ResultFormatGlobalState &gstate,
	                                             ResultFormatLocalState &lstate) override;

public:
	DUCKDB_API static unique_ptr<ArrowPayload> UnpackUnit(unique_ptr<ResultUnit> unit);
	DUCKDB_API static unique_ptr<ArrowPayload> CopyPayload(const ArrowPayload &payload);

private:
	//! The rows an array holds, except at a batch boundary and at a producer's end of input
	idx_t batch_size;
};

} // namespace duckdb
