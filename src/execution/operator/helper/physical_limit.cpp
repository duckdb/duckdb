#include "duckdb/execution/operator/helper/physical_limit.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

constexpr const idx_t PhysicalLimit::MAX_LIMIT_VALUE;

PhysicalLimit::PhysicalLimit(PhysicalPlan &physical_plan, vector<LogicalType> types, BoundLimitNode limit_val_p,
                             BoundLimitNode offset_val_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::LIMIT, std::move(types), estimated_cardinality),
      limit_val(std::move(limit_val_p)), offset_val(std::move(offset_val_p)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class LimitGlobalState : public GlobalSinkState {
public:
	explicit LimitGlobalState(ClientContext &context, const PhysicalLimit &op)
	    : data(context, op.types, ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
		limit = 0;
		offset = 0;
	}

	mutex glock;
	idx_t limit;
	idx_t offset;
	BatchedDataCollection data;
};

class LimitLocalState : public LocalSinkState {
public:
	explicit LimitLocalState(ClientContext &context, const PhysicalLimit &op)
	    : current_offset(0), data(context, op.types, ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
		PhysicalLimit::SetInitialLimits(op.limit_val, op.offset_val, limit, offset);
	}

	idx_t current_offset;
	optional_idx limit;
	optional_idx offset;
	BatchedDataCollection data;
};

unique_ptr<GlobalSinkState> PhysicalLimit::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LimitGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalLimit::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<LimitLocalState>(context.client, *this);
}

void PhysicalLimit::SetInitialLimits(const BoundLimitNode &limit_val, const BoundLimitNode &offset_val,
                                     optional_idx &limit, optional_idx &offset) {
	switch (limit_val.Type()) {
	case LimitNodeType::CONSTANT_VALUE:
		limit = limit_val.GetConstantValue();
		break;
	case LimitNodeType::UNSET:
		limit = MAX_LIMIT_VALUE;
		break;
	default:
		break;
	}
	switch (offset_val.Type()) {
	case LimitNodeType::CONSTANT_VALUE:
		offset = offset_val.GetConstantValue();
		break;
	case LimitNodeType::UNSET:
		offset = 0;
		break;
	default:
		break;
	}
}

bool PhysicalLimit::ComputeOffset(ExecutionContext &context, DataChunk &input, optional_idx &limit,
                                  optional_idx &offset, idx_t current_offset, idx_t &max_element,
                                  const BoundLimitNode &limit_val, const BoundLimitNode &offset_val) {
	if (!limit.IsValid()) {
		Value val = GetDelimiter(context, input, limit_val.GetValueExpression());
		if (!val.IsNull()) {
			limit = val.GetValue<idx_t>();
		} else {
			limit = MAX_LIMIT_VALUE;
		}
		if (limit.GetIndex() > MAX_LIMIT_VALUE) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", limit.GetIndex(), MAX_LIMIT_VALUE);
		}
	}
	if (!offset.IsValid()) {
		Value val = GetDelimiter(context, input, offset_val.GetValueExpression());
		if (!val.IsNull()) {
			offset = val.GetValue<idx_t>();
		} else {
			offset = 0;
		}
		if (offset.GetIndex() > MAX_LIMIT_VALUE) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", offset.GetIndex(), MAX_LIMIT_VALUE);
		}
	}
	max_element = limit.GetIndex() + offset.GetIndex();
	if (limit == 0 || current_offset >= max_element) {
		return false;
	}
	return true;
}

SinkResultType PhysicalLimit::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	D_ASSERT(chunk.size() > 0);
	auto &state = input.local_state.Cast<LimitLocalState>();
	auto &limit = state.limit;
	auto &offset = state.offset;

	idx_t max_element;
	if (!ComputeOffset(context, chunk, limit, offset, state.current_offset, max_element, limit_val, offset_val)) {
		return SinkResultType::FINISHED;
	}
	auto max_cardinality = max_element - state.current_offset;
	if (max_cardinality < chunk.size()) {
		chunk.SetCardinality(max_cardinality);
	}
	state.data.Append(chunk, state.partition_info.batch_index.GetIndex());
	state.current_offset += chunk.size();
	if (state.current_offset == max_element) {
		return SinkResultType::FINISHED;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalLimit::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<LimitGlobalState>();
	auto &state = input.local_state.Cast<LimitLocalState>();

	lock_guard<mutex> lock(gstate.glock);
	if (state.limit.IsValid()) {
		gstate.limit = state.limit.GetIndex();
	}
	if (state.offset.IsValid()) {
		gstate.offset = state.offset.GetIndex();
	}
	gstate.data.Merge(state.data);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class LimitSourceState : public GlobalSourceState {
public:
	LimitSourceState() {
		initialized = false;
		current_offset = 0;
	}

	bool initialized;
	idx_t current_offset;
	BatchedChunkScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalLimit::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<LimitSourceState>();
}

SourceResultType PhysicalLimit::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<LimitGlobalState>();
	auto &state = input.global_state.Cast<LimitSourceState>();
	while (state.current_offset < gstate.limit + gstate.offset) {
		if (!state.initialized) {
			gstate.data.InitializeScan(state.scan_state);
			state.initialized = true;
		}
		gstate.data.Scan(state.scan_state, chunk);
		if (chunk.size() == 0) {
			return SourceResultType::FINISHED;
		}
		if (HandleOffset(chunk, state.current_offset, gstate.offset, gstate.limit)) {
			break;
		}
	}

	return chunk.size() > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

bool PhysicalLimit::HandleOffset(DataChunk &input, idx_t &current_offset, idx_t offset, idx_t limit) {
	idx_t max_element = limit + offset;
	if (limit == DConstants::INVALID_INDEX) {
		max_element = DConstants::INVALID_INDEX;
	}
	idx_t input_size = input.size();
	if (current_offset < offset) {
		// we are not yet at the offset point
		if (current_offset + input.size() > offset) {
			// however we will reach it in this chunk
			// we have to copy part of the chunk with an offset
			idx_t start_position = offset - current_offset;
			auto chunk_count = MinValue<idx_t>(limit, input.size() - start_position);
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			for (idx_t i = 0; i < chunk_count; i++) {
				sel.set_index(i, start_position + i);
			}
			// set up a slice of the input chunks
			input.Slice(input, sel, chunk_count);
		} else {
			current_offset += input_size;
			return false;
		}
	} else {
		// have to copy either the entire chunk or part of it
		idx_t chunk_count;
		if (current_offset + input.size() >= max_element) {
			// have to limit the count of the chunk
			chunk_count = max_element - current_offset;
		} else {
			// we copy the entire chunk
			chunk_count = input.size();
		}
		// instead of copying we just change the pointer in the current chunk
		input.Reference(input);
		input.SetCardinality(chunk_count);
	}

	current_offset += input_size;
	return true;
}

Value PhysicalLimit::GetDelimiter(ExecutionContext &context, DataChunk &input, const Expression &expr) {
	DataChunk limit_chunk;
	vector<LogicalType> types {expr.return_type};
	auto &allocator = Allocator::Get(context.client);
	limit_chunk.Initialize(allocator, types);
	ExpressionExecutor limit_executor(context.client, &expr);
	auto input_size = input.size();
	input.SetCardinality(1);
	limit_executor.Execute(input, limit_chunk);
	input.SetCardinality(input_size);
	auto limit_value = limit_chunk.GetValue(0, 0);
	return limit_value;
}

} // namespace duckdb
