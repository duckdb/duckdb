//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_refresh_feature.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/append_state.hpp"

namespace duckdb {

class RefreshFeatureGlobalState : public GlobalSinkState {
public:
	RefreshFeatureGlobalState() : new_version(0), retain_versions(0), insert_count(0) {
	}
	mutex lock;
	//! The newly-created version table the refreshed rows are appended into.
	optional_ptr<DuckTableEntry> table;
	//! Location of the feature (for the version bump and version-table GC in the source phase).
	string catalog_name;
	string schema_name;
	int64_t new_version;
	int64_t retain_versions;
	//! Rows reported as rows_affected: the recomputed rows only, not the rows copied forward unchanged.
	idx_t insert_count;
};

class RefreshFeatureLocalState : public LocalSinkState {
public:
	RefreshFeatureLocalState() : collection_index(DConstants::INVALID_INDEX), recomputed_count(0) {
	}
	//! Per-thread optimistic writer and row group collection for parallel appends.
	TableAppendState local_append_state;
	PhysicalIndex collection_index;
	unique_ptr<OptimisticDataWriter> optimistic_writer;
	//! Rows this thread saw with the recomputed marker set — accumulated into the global count in Combine.
	idx_t recomputed_count;
};

//! PhysicalRefreshFeature represents a REFRESH FEATURE command. It materializes its child (the next
//! version's contents) into a new version table, garbage-collects versions beyond the retain limit,
//! and bumps the feature's current version — all inside the statement's own transaction.
class PhysicalRefreshFeature : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::REFRESH_FEATURE;

public:
	PhysicalRefreshFeature(PhysicalPlan &physical_plan, string feature_name, vector<string> result_names,
	                       vector<LogicalType> result_types, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::REFRESH_FEATURE, {LogicalType::BIGINT},
	                       estimated_cardinality),
	      feature_name(std::move(feature_name)), result_names(std::move(result_names)),
	      result_types(std::move(result_types)) {
	}

	string feature_name;
	vector<string> result_names;
	vector<LogicalType> result_types;

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
