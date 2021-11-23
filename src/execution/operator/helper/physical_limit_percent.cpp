#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"

#include "duckdb/common/algorithm.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class LimitPercentGlobalState : public GlobalSinkState {
public:
	explicit LimitPercentGlobalState(const PhysicalLimitPercent &op) : current_offset(0) {
		if (!op.limit_expression) {
			this->limit_percent = op.limit_percent;
			is_limit_percent_delimited = true;
		}

		if (!op.offset_expression) {
			this->offset = op.offset_value;
			is_offset_delimited = true;
		}
	}

	idx_t current_offset;
	idx_t limit;
	double limit_percent;
	idx_t offset;
	ChunkCollection data;

	bool is_limit_percent_delimited = false;
	bool is_offset_delimited = false;
};

Value GetDelimiter(DataChunk &input, Expression *expr, Value original_value) {
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
	return limit_value;
}

unique_ptr<GlobalSinkState> PhysicalLimitPercent::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<LimitPercentGlobalState>(*this);
}

SinkResultType PhysicalLimitPercent::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                          DataChunk &input) const {
	D_ASSERT(input.size() > 0);
	auto &state = (LimitPercentGlobalState &)gstate;
	auto &limit = state.limit;
	auto &limit_percent = state.limit_percent;
	auto &offset = state.offset;

	if (limit != INVALID_INDEX && offset != INVALID_INDEX) {
		idx_t max_element = limit + offset;
		if ((limit == 0 || state.current_offset >= max_element) && !(limit_expression || offset_expression)) {
			return SinkResultType::FINISHED;
		}
	}

	// get the next chunk from the child
	if (!state.is_limit_percent_delimited) {
		Value val;
		val.value_.double_ = 100.0;
		val = GetDelimiter(input, limit_expression.get(), val);
		limit_percent = val.value_.double_;
		if (limit_percent < 0.0) {
			throw BinderException("Percentage value(%f) can't be negative", limit_percent);
		}
		state.is_limit_percent_delimited = true;
	}
	if (state.is_limit_percent_delimited && limit_count != INVALID_INDEX) {
		limit = MinValue((idx_t)(limit_percent / 100 * limit_count), limit_count);
	}
	if (!state.is_offset_delimited) {
		Value val;
		val.value_.ubigint = 1ULL << 62ULL;
		val = GetDelimiter(input, offset_expression.get(), val);
		offset = val.value_.ubigint;
		if (offset > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", offset, 1ULL << 62ULL);
		}
		state.is_offset_delimited = true;
	}
	idx_t max_element = limit + offset;
	if (limit == INVALID_INDEX) {
		max_element = INVALID_INDEX;
	}
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
class LimitPercentOperatorState : public GlobalSourceState {
public:
	LimitPercentOperatorState() : chunk_idx(0) {
	}

	idx_t chunk_idx;
};

unique_ptr<GlobalSourceState> PhysicalLimitPercent::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<LimitPercentOperatorState>();
}

void PhysicalLimitPercent::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                   LocalSourceState &lstate) const {
	auto &gstate = (LimitPercentGlobalState &)*sink_state;
	auto &state = (LimitPercentOperatorState &)gstate_p;
	if (state.chunk_idx >= gstate.data.ChunkCount()) {
		return;
	}
	chunk.Reference(gstate.data.GetChunk(state.chunk_idx));
	state.chunk_idx++;
}

} // namespace duckdb
