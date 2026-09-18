//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/runtime_filter_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

class ClientContext;
class Expression;
struct JoinFilterPushdownColumn;

enum class RuntimeFilterCastMode : uint8_t { DEFAULT_CAST, TRY_CAST };

struct RuntimeFilterCastStep {
	RuntimeFilterCastStep(LogicalType target_type_p, RuntimeFilterCastMode mode_p)
	    : target_type(std::move(target_type_p)), mode(mode_p) {
	}

	LogicalType target_type;
	RuntimeFilterCastMode mode;
};

//! Utilities for the cast chains recorded on pushed runtime filter columns.
//! Shared by the join filter pushdown (physical_hash_join) and the TopN dynamic filter pushdown.
struct RuntimeFilterCastUtil {
	//! Whether a cast from source_type to target_type can fail at runtime
	static bool RuntimeFilterCastCanFail(const LogicalType &source_type, const LogicalType &target_type);
	//! Whether the cast chain of this column contains (or requires) a try cast
	static bool RuntimeFilterUsesTryCast(const JoinFilterPushdownColumn &column);
	//! The type that CreateRuntimeFilterInputExpression evaluates to for this column
	static LogicalType GetRuntimeFilterInputType(const JoinFilterPushdownColumn &column,
	                                             const LogicalType &runtime_type);
	//! Whether evaluating the pushed runtime filter requires reconstructing the pushed expression
	//! on top of the raw scan value (i.e. the cast chain contains a cast that can fail)
	static bool RequiresRuntimeFilterExpressionReconstruction(const JoinFilterPushdownColumn &column,
	                                                          const LogicalType &runtime_type);
	//! Build the input expression for evaluating a pushed runtime filter on top of the raw scan column:
	//! a BoundReferenceExpression(0) in the storage type, followed by the recorded cast chain.
	//! Set `preserves_cast_errors` when a default (non-try) cast in the chain can fail.
	static unique_ptr<Expression> CreateRuntimeFilterInputExpression(ClientContext &context,
	                                                                 const JoinFilterPushdownColumn &column,
	                                                                 bool &preserves_cast_errors);
};

} // namespace duckdb
