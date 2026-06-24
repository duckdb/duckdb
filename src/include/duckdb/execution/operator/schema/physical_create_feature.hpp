#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_feature_info.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/append_state.hpp"

namespace duckdb {

class CreateFeatureGlobalState : public GlobalSinkState {
public:
	CreateFeatureGlobalState() : insert_count(0) {
	}
	mutex lock;
	optional_ptr<DuckTableEntry> table;
	idx_t insert_count;
};

class CreateFeatureLocalState : public LocalSinkState {
public:
	CreateFeatureLocalState() : collection_index(DConstants::INVALID_INDEX) {
	}
	//! Per-thread optimistic writer and row group collection for parallel appends.
	TableAppendState local_append_state;
	PhysicalIndex collection_index;
	unique_ptr<OptimisticDataWriter> optimistic_writer;
};

//! PhysicalCreateFeature represents a CREATE FEATURE command
class PhysicalCreateFeature : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_FEATURE;

public:
	explicit PhysicalCreateFeature(PhysicalPlan &physical_plan, unique_ptr<CreateFeatureInfo> info,
	                               idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_FEATURE, {LogicalType::BIGINT},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<CreateFeatureInfo> info;

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
