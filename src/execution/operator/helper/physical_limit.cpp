#include "duckdb/execution/operator/helper/physical_limit.hpp"

#include "duckdb/common/algorithm.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class LimitGlobalState : public GlobalSinkState {
public:
	explicit LimitGlobalState(const PhysicalLimit &op) : current_offset(0) {
		this->limit = op.limit_expression ? DConstants::INVALID_INDEX : op.limit_value;
		this->offset = op.offset_expression ? DConstants::INVALID_INDEX : op.offset_value;
	}

	idx_t current_offset;
	idx_t limit;
	idx_t offset;
	ChunkCollection data;
};

uint64_t GetDelimiter(DataChunk &input, Expression *expr, uint64_t original_value) {
	DataChunk limit_chunk;
	vector<LogicalType> types {expr->return_type};
	limit_chunk.Initialize(types);
	ExpressionExecutor limit_executor(expr);
	auto input_size = input.size();
	input.SetCardinality(1);
	limit_executor.Execute(input, limit_chunk);
	input.SetCardinality(input_size);
	auto limit_value = limit_chunk.GetValue(0, 0);
	if (limit_value.is_null) {
		return original_value;
	}
	if (limit_value.value_.ubigint > 1ULL << 62ULL) {
		throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", original_value, 1ULL << 62ULL);
	}
	return limit_value.value_.ubigint;
}

unique_ptr<GlobalSinkState> PhysicalLimit::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<LimitGlobalState>(*this);
}

SinkResultType PhysicalLimit::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                   DataChunk &input) const {
	D_ASSERT(input.size() > 0);
	auto &state = (LimitGlobalState &)gstate;
	auto &limit = state.limit;
	auto &offset = state.offset;

	if (limit != DConstants::INVALID_INDEX && offset != DConstants::INVALID_INDEX) {
		idx_t max_element = limit + offset;
		if ((limit == 0 || state.current_offset >= max_element) && !(limit_expression || offset_expression)) {
			return SinkResultType::FINISHED;
		}
	}

	// get the next chunk from the child
	if (limit == DConstants::INVALID_INDEX) {
		limit = GetDelimiter(input, limit_expression.get(), 1ULL << 62ULL);
	}
	if (offset == DConstants::INVALID_INDEX) {
		offset = GetDelimiter(input, offset_expression.get(), 0);
	}
	idx_t max_element = limit + offset;
	idx_t input_size = input.size();
	if (limit == 0 || state.current_offset >= max_element) {
		return SinkResultType::FINISHED;
	}
	if (state.current_offset < offset) {
		// we are not yet at the offset point
		if (state.current_offset + input.size() > offset) {
			// however we will reach it in this chunk
			// we have to copy part of the chunk with an offset
			idx_t start_position = offset - state.current_offset;
			auto chunk_count = MinValue<idx_t>(limit, input.size() - start_position);
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			for (idx_t i = 0; i < chunk_count; i++) {
				sel.set_index(i, start_position + i);
			}
			// set up a slice of the input chunks
			input.Slice(input, sel, chunk_count);
		} else {
			state.current_offset += input_size;
			return SinkResultType::NEED_MORE_INPUT;
		}
	} else {
		// have to copy either the entire chunk or part of it
		idx_t chunk_count;
		if (state.current_offset + input.size() >= max_element) {
			// have to limit the count of the chunk
			chunk_count = max_element - state.current_offset;
		} else {
			// we copy the entire chunk
			chunk_count = input.size();
		}
		// instead of copying we just change the pointer in the current chunk
		input.Reference(input);
		input.SetCardinality(chunk_count);
	}

	state.current_offset += input_size;
	state.data.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class LimitOperatorState : public GlobalSourceState {
public:
	LimitOperatorState() : chunk_idx(0) {
	}

	idx_t chunk_idx;
};

unique_ptr<GlobalSourceState> PhysicalLimit::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<LimitOperatorState>();
}

void PhysicalLimit::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                            LocalSourceState &lstate) const {
	auto &gstate = (LimitGlobalState &)*sink_state;
	auto &state = (LimitOperatorState &)gstate_p;
	if (state.chunk_idx >= gstate.data.ChunkCount()) {
		return;
	}
	chunk.Reference(gstate.data.GetChunk(state.chunk_idx));
	state.chunk_idx++;
}

} // namespace duckdb
