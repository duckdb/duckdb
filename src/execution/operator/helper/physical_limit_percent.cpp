#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"
#include "duckdb/execution/operator/helper/physical_limit.hpp"

#include "duckdb/common/algorithm.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class LimitPercentGlobalState : public GlobalSinkState {
public:
	explicit LimitPercentGlobalState(ClientContext &context, const PhysicalLimitPercent &op)
	    : current_offset(0), data(context, op.GetTypes()) {
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
			this->offset = 0;
		}
	}

	idx_t current_offset;
	double limit_percent;
	idx_t offset;
	ColumnDataCollection data;

	bool is_limit_percent_delimited = false;
	bool is_offset_delimited = false;
};

unique_ptr<GlobalSinkState> PhysicalLimitPercent::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<LimitPercentGlobalState>(context, *this);
}

SinkResultType PhysicalLimitPercent::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                          DataChunk &input) const {
	D_ASSERT(input.size() > 0);
	auto &state = (LimitPercentGlobalState &)gstate;
	auto &limit_percent = state.limit_percent;
	auto &offset = state.offset;

	// get the next chunk from the child
	if (!state.is_limit_percent_delimited) {
		Value val = PhysicalLimit::GetDelimiter(context, input, limit_expression.get());
		if (!val.IsNull()) {
			limit_percent = val.GetValue<double>();
		}
		if (limit_percent < 0.0) {
			throw BinderException("Percentage value(%f) can't be negative", limit_percent);
		}
		state.is_limit_percent_delimited = true;
	}
	if (!state.is_offset_delimited) {
		Value val = PhysicalLimit::GetDelimiter(context, input, offset_expression.get());
		if (!val.IsNull()) {
			offset = val.GetValue<idx_t>();
		}
		if (offset > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", offset, 1ULL << 62ULL);
		}
		state.is_offset_delimited = true;
	}

	if (!PhysicalLimit::HandleOffset(input, state.current_offset, offset, DConstants::INVALID_INDEX)) {
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
	explicit LimitPercentOperatorState(const PhysicalLimitPercent &op)
	    : limit(DConstants::INVALID_INDEX), current_offset(0) {
		auto &gstate = (LimitPercentGlobalState &)*op.sink_state;
		gstate.data.InitializeScan(scan_state);
	}

	ColumnDataScanState scan_state;
	idx_t limit;
	idx_t current_offset;
};

unique_ptr<GlobalSourceState> PhysicalLimitPercent::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<LimitPercentOperatorState>(*this);
}

void PhysicalLimitPercent::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                   LocalSourceState &lstate) const {
	auto &gstate = (LimitPercentGlobalState &)*sink_state;
	auto &state = (LimitPercentOperatorState &)gstate_p;
	auto &percent_limit = gstate.limit_percent;
	auto &offset = gstate.offset;
	auto &limit = state.limit;
	auto &current_offset = state.current_offset;

	if (gstate.is_limit_percent_delimited && limit == DConstants::INVALID_INDEX) {
		idx_t count = gstate.data.Count();
		if (count > 0) {
			count += offset;
		}
		if (Value::IsNan(percent_limit) || percent_limit < 0 || percent_limit > 100) {
			throw OutOfRangeException("Limit percent out of range, should be between 0% and 100%");
		}
		double limit_dbl = percent_limit / 100 * count;
		if (limit_dbl > count) {
			limit = count;
		} else {
			limit = idx_t(limit_dbl);
		}
		if (limit == 0) {
			return;
		}
	}

	if (current_offset >= limit) {
		return;
	}
	if (!gstate.data.Scan(state.scan_state, chunk)) {
		return;
	}

	PhysicalLimit::HandleOffset(chunk, current_offset, 0, limit);
}

} // namespace duckdb
