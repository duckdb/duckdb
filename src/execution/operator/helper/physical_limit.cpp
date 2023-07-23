#include "duckdb/execution/operator/helper/physical_limit.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/helper/physical_streaming_limit.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

PhysicalLimit::PhysicalLimit(vector<LogicalType> types, idx_t limit, idx_t offset,
                             unique_ptr<Expression> limit_expression, unique_ptr<Expression> offset_expression,
                             idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::LIMIT, std::move(types), estimated_cardinality), limit_value(limit),
      offset_value(offset), limit_expression(std::move(limit_expression)),
      offset_expression(std::move(offset_expression)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class LimitGlobalState : public GlobalSinkState {
public:
	explicit LimitGlobalState(ClientContext &context, const PhysicalLimit &op) : data(context, op.types, true) {
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
	    : current_offset(0), data(context, op.types, true) {
		this->limit = op.limit_expression ? DConstants::INVALID_INDEX : op.limit_value;
		this->offset = op.offset_expression ? DConstants::INVALID_INDEX : op.offset_value;
	}

	idx_t current_offset;
	idx_t limit;
	idx_t offset;
	BatchedDataCollection data;
};

unique_ptr<GlobalSinkState> PhysicalLimit::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LimitGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalLimit::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<LimitLocalState>(context.client, *this);
}

bool PhysicalLimit::ComputeOffset(ExecutionContext &context, DataChunk &input, idx_t &limit, idx_t &offset,
                                  idx_t current_offset, idx_t &max_element, Expression *limit_expression,
                                  Expression *offset_expression) {
	if (limit != DConstants::INVALID_INDEX && offset != DConstants::INVALID_INDEX) {
		max_element = limit + offset;
		if ((limit == 0 || current_offset >= max_element) && !(limit_expression || offset_expression)) {
			return false;
		}
	}

	// get the next chunk from the child
	if (limit == DConstants::INVALID_INDEX) {
		limit = 1ULL << 62ULL;
		Value val = GetDelimiter(context, input, limit_expression);
		if (!val.IsNull()) {
			limit = val.GetValue<idx_t>();
		}
		if (limit > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", limit, 1ULL << 62ULL);
		}
	}
	if (offset == DConstants::INVALID_INDEX) {
		offset = 0;
		Value val = GetDelimiter(context, input, offset_expression);
		if (!val.IsNull()) {
			offset = val.GetValue<idx_t>();
		}
		if (offset > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", offset, 1ULL << 62ULL);
		}
	}
	max_element = limit + offset;
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
	if (!ComputeOffset(context, chunk, limit, offset, state.current_offset, max_element, limit_expression.get(),
	                   offset_expression.get())) {
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
	gstate.limit = state.limit;
	gstate.offset = state.offset;
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

SourceResultType PhysicalLimit::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
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

Value PhysicalLimit::GetDelimiter(ExecutionContext &context, DataChunk &input, Expression *expr) {
	DataChunk limit_chunk;
	vector<LogicalType> types {expr->return_type};
	auto &allocator = Allocator::Get(context.client);
	limit_chunk.Initialize(allocator, types);
	ExpressionExecutor limit_executor(context.client, expr);
	auto input_size = input.size();
	input.SetCardinality(1);
	limit_executor.Execute(input, limit_chunk);
	input.SetCardinality(input_size);
	auto limit_value = limit_chunk.GetValue(0, 0);
	return limit_value;
}

} // namespace duckdb
