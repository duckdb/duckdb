//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

//! PhysicalCreateFunction represents a CREATE FUNCTION command
class PhysicalCreateFunction : public PhysicalOperator {
public:
	explicit PhysicalCreateFunction(unique_ptr<CreateMacroInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_MACRO, {LogicalType::BIGINT}), info(move(info)) {
	}

	unique_ptr<CreateMacroInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};

} // namespace duckdb
