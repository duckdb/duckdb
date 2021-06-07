#include "duckdb/execution/operator/helper/physical_limit.hpp"

#include "duckdb/common/algorithm.hpp"

#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

class PhysicalLimitOperatorState : public PhysicalOperatorState {
public:
	PhysicalLimitOperatorState(PhysicalOperator &op, PhysicalOperator *child, idx_t current_offset = 0)
	    : PhysicalOperatorState(op, child), current_offset(current_offset) {
	}

	idx_t current_offset;
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
	return limit_value.value_.ubigint;
}

void PhysicalLimit::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                     PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalLimitOperatorState *>(state_p);

	idx_t max_element = limit + offset;
	if ((limit == 0 || state->current_offset >= max_element) && !(limit_expression || offset_expression)) {
		return;
	}

	// get the next chunk from the child
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (limit_expression) {
			limit = GetDelimiter(state->child_chunk, limit_expression.get(), limit);
			limit_expression.reset();
		}
		if (offset_expression) {
			offset = GetDelimiter(state->child_chunk, offset_expression.get(), offset);
			offset_expression.reset();
		}
		max_element = limit + offset;
		if (state->child_chunk.size() == 0) {
			return;
		}
		if (limit == 0 || state->current_offset >= max_element) {
			return;
		}
		if (state->current_offset < offset) {
			// we are not yet at the offset point
			if (state->current_offset + state->child_chunk.size() > offset) {
				// however we will reach it in this chunk
				// we have to copy part of the chunk with an offset
				idx_t start_position = offset - state->current_offset;
				auto chunk_count = MinValue<idx_t>(limit, state->child_chunk.size() - start_position);
				SelectionVector sel(STANDARD_VECTOR_SIZE);
				for (idx_t i = 0; i < chunk_count; i++) {
					sel.set_index(i, start_position + i);
				}
				// set up a slice of the input chunks
				chunk.Slice(state->child_chunk, sel, chunk_count);
			}
		} else {
			// have to copy either the entire chunk or part of it
			idx_t chunk_count;
			if (state->current_offset + state->child_chunk.size() >= max_element) {
				// have to limit the count of the chunk
				chunk_count = max_element - state->current_offset;
			} else {
				// we copy the entire chunk
				chunk_count = state->child_chunk.size();
			}
			// instead of copying we just change the pointer in the current chunk
			chunk.Reference(state->child_chunk);
			chunk.SetCardinality(chunk_count);
		}

		state->current_offset += state->child_chunk.size();
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalLimit::GetOperatorState() {
	return make_unique<PhysicalLimitOperatorState>(*this, children[0].get(), 0);
}

} // namespace duckdb
