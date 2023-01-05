#include "duckdb/execution/operator/join/physical_pos_join.hpp"

#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

namespace duckdb {

PhysicalPositionalJoin::PhysicalPositionalJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
                                               unique_ptr<PhysicalOperator> right, idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::POSITIONAL_JOIN, move(types), estimated_cardinality) {
	children.push_back(move(left));
	children.push_back(move(right));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PositionalJoinGlobalState : public GlobalSinkState {
public:
	explicit PositionalJoinGlobalState(ClientContext &context, const PhysicalPositionalJoin &op)
	    : rhs_materialized(context, op.children[1]->GetTypes()) {
		rhs_materialized.InitializeAppend(append_state);
	}

	ColumnDataCollection rhs_materialized;
	ColumnDataAppendState append_state;
	mutex rhs_lock;
};

unique_ptr<GlobalSinkState> PhysicalPositionalJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<PositionalJoinGlobalState>(context, *this);
}

SinkResultType PhysicalPositionalJoin::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                            DataChunk &input) const {
	auto &sink = (PositionalJoinGlobalState &)state;
	lock_guard<mutex> client_guard(sink.rhs_lock);
	sink.rhs_materialized.Append(sink.append_state, input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PositionalJoinExecutor {
public:
	explicit PositionalJoinExecutor(ColumnDataCollection &rhs);

	OperatorResultType Execute(DataChunk &input, DataChunk &output);

private:
	void Reset(DataChunk &input, DataChunk &output);

private:
	ColumnDataCollection &rhs;
	ColumnDataScanState scan_state;
	DataChunk scan_chunk;
	idx_t source_offset;
	bool initialized;
	bool scan_input_chunk;
};

PositionalJoinExecutor::PositionalJoinExecutor(ColumnDataCollection &rhs)
    : rhs(rhs), source_offset(0), initialized(false) {
	rhs.InitializeScanChunk(scan_chunk);
}

void PositionalJoinExecutor::Reset(DataChunk &input, DataChunk &output) {
	initialized = true;
	scan_input_chunk = false;
	rhs.InitializeScan(scan_state);
	source_offset = 0;
	scan_chunk.Reset();
}

OperatorResultType PositionalJoinExecutor::Execute(DataChunk &input, DataChunk &output) {
	if (!initialized) {
		// not initialized yet: initialize the scan
		Reset(input, output);
	}

	// Consume the entire input by repeatedly scanning
	// Stop if we run out of scan and return a truncated block

	// Reference the input and assume it will be full
	const auto input_column_count = input.ColumnCount();
	auto chunk_size = input.size();
	for (idx_t i = 0; i < input_column_count; ++i) {
		output.data[i].Reference(input.data[i]);
	}

	if (source_offset >= scan_chunk.size()) {
		scan_chunk.Reset();
		rhs.Scan(scan_state, scan_chunk);
		source_offset = 0;
	}

	// Fast track: aligned chunks
	if (!source_offset && scan_chunk.size() >= chunk_size) {
		for (idx_t i = 0; i < scan_chunk.ColumnCount(); ++i) {
			output.data[input_column_count + i].Reference(scan_chunk.data[i]);
		}
		output.SetCardinality(chunk_size);
		source_offset = chunk_size;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Ragged values from scan - append until we are done
	idx_t target_offset = 0;
	while (target_offset < chunk_size) {
		if (scan_chunk.size() == 0) {
			break;
		}
		const auto remaining = chunk_size - target_offset;
		const auto available = scan_chunk.size() - source_offset;
		const auto copy_size = MinValue(remaining, available);
		const auto source_count = source_offset + copy_size;
		for (idx_t i = 0; i < scan_chunk.ColumnCount(); ++i) {
			VectorOperations::Copy(scan_chunk.data[i], output.data[input_column_count + i], source_count, source_offset,
			                       target_offset);
		}
		source_offset += copy_size;
		target_offset += copy_size;
		if (source_offset >= scan_chunk.size()) {
			scan_chunk.Reset();
			rhs.Scan(scan_state, scan_chunk);
			source_offset = 0;
		}
	}
	output.SetCardinality(target_offset);

	return target_offset > 0 ? OperatorResultType::NEED_MORE_INPUT : OperatorResultType::FINISHED;
}

class PositionalJoinOperatorState : public CachingOperatorState {
public:
	explicit PositionalJoinOperatorState(ColumnDataCollection &rhs) : executor(rhs) {
	}

	PositionalJoinExecutor executor;
};

unique_ptr<OperatorState> PhysicalPositionalJoin::GetOperatorState(ExecutionContext &context) const {
	auto &sink = (PositionalJoinGlobalState &)*sink_state;
	return make_unique<PositionalJoinOperatorState>(sink.rhs_materialized);
}

OperatorResultType PhysicalPositionalJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input,
                                                           DataChunk &chunk, GlobalOperatorState &gstate,
                                                           OperatorState &state_p) const {
	auto &state = (PositionalJoinOperatorState &)state_p;
	return state.executor.Execute(input, chunk);
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalPositionalJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, *this);
}

vector<const PhysicalOperator *> PhysicalPositionalJoin::GetSources() const {
	return children[0]->GetSources();
}

} // namespace duckdb
