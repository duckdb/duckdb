#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"
#include "duckdb/execution/operator/helper/physical_limit.hpp"

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
		} else {
			this->limit_percent = 100.0;
		}

		if (!op.offset_expression) {
			this->offset = op.offset_value;
			is_offset_delimited = true;
		} else {
			this->offset = INVALID_INDEX;
		}
	}

	idx_t current_offset;
	double limit_percent;
	idx_t offset;
	ChunkCollection data;

	bool is_limit_percent_delimited = false;
	bool is_offset_delimited = false;
};

unique_ptr<GlobalSinkState> PhysicalLimitPercent::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<LimitPercentGlobalState>(*this);
}

SinkResultType PhysicalLimitPercent::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                          DataChunk &input) const {
	D_ASSERT(input.size() > 0);
	auto &state = (LimitPercentGlobalState &)gstate;
	auto &limit_percent = state.limit_percent;
	auto &offset = state.offset;

	// get the next chunk from the child
	if (!state.is_limit_percent_delimited) {
		Value val = PhysicalLimit::GetDelimiter(input, limit_expression.get());
		if (!val.is_null) {
			limit_percent = val.GetValue<double>();
		}
		if (limit_percent < 0.0) {
			throw BinderException("Percentage value(%f) can't be negative", limit_percent);
		}
		state.is_limit_percent_delimited = true;
	}
	if (!state.is_offset_delimited) {
		Value val = PhysicalLimit::GetDelimiter(input, offset_expression.get());
		if (!val.is_null) {
			offset = val.GetValue<idx_t>();
		}
		if (offset > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", offset, 1ULL << 62ULL);
		}
		state.is_offset_delimited = true;
	}

	if (!PhysicalLimit::HandleOffset(input, state.current_offset, offset, INVALID_INDEX)) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	state.data.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class LimitPercentOperatorState : public GlobalSourceState {
public:
	LimitPercentOperatorState() : chunk_idx(0), limit(INVALID_INDEX) {
	}

	idx_t chunk_idx;
	idx_t limit;
};

unique_ptr<GlobalSourceState> PhysicalLimitPercent::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<LimitPercentOperatorState>();
}

void PhysicalLimitPercent::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                   LocalSourceState &lstate) const {
	auto &gstate = (LimitPercentGlobalState &)*sink_state;
	auto &state = (LimitPercentOperatorState &)gstate_p;
	auto &limit_percent = gstate.limit_percent;
	auto &limit = state.limit;

	if (gstate.is_limit_percent_delimited && limit == INVALID_INDEX) {
		idx_t count = gstate.data.Count();
		limit = MinValue((idx_t)(limit_percent / 100 * count), count);
		if (limit == 0) {
			return;
		}
	}

	if (state.chunk_idx >= gstate.data.ChunkCount() || limit <= 0) {
		return;
	}

	DataChunk &input = gstate.data.GetChunk(state.chunk_idx);
	idx_t chunk_count = MinValue(limit, input.size());
	limit -= chunk_count;
	if (chunk_count < input.size()) {
		input.Reference(input);
		input.SetCardinality(chunk_count);
	} else {
		state.chunk_idx++;
	}

	chunk.Reference(input);
}

} // namespace duckdb
