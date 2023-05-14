//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

//! PhysicalCreateType represents a CREATE TYPE command
class PhysicalCreateType : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_TYPE;

public:
	explicit PhysicalCreateType(unique_ptr<CreateTypeInfo> info, idx_t estimated_cardinality);

	unique_ptr<CreateTypeInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	bool IsSink() const override {
		return !children.empty();
	}

	bool ParallelSink() const override {
		return false;
	}

	bool SinkOrderDependent() const override {
		return true;
	}
};

} // namespace duckdb
