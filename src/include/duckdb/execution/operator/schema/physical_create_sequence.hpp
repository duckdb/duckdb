//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_sequence.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"

namespace duckdb {

//! PhysicalCreateSequence represents a CREATE SEQUENCE command
class PhysicalCreateSequence : public PhysicalOperator {
public:
	PhysicalCreateSequence(unique_ptr<CreateSequenceInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_SEQUENCE, {TypeId::BOOL}), info(move(info)) {
	}

	unique_ptr<CreateSequenceInfo> info;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};

} // namespace duckdb
