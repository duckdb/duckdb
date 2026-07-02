#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/helper/physical_limit.hpp"

namespace duckdb {

PhysicalLimitPercent::PhysicalLimitPercent(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                           BoundLimitNode limit_val_p, BoundLimitNode offset_val_p,
                                           idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::LIMIT_PERCENT, std::move(types), estimated_cardinality),
      limit_val(std::move(limit_val_p)), offset_val(std::move(offset_val_p)) {
	D_ASSERT(limit_val.Type() == LimitNodeType::CONSTANT_PERCENTAGE ||
	         limit_val.Type() == LimitNodeType::EXPRESSION_PERCENTAGE);
}
//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class LimitPercentGlobalState : public GlobalSinkState {
public:
	explicit LimitPercentGlobalState(ClientContext &context, const PhysicalLimitPercent &op)
	    : current_offset(0), data(context, op.GetTypes()) {
		switch (op.limit_val.Type()) {
		case LimitNodeType::CONSTANT_PERCENTAGE:
			this->limit_percent = op.limit_val.GetConstantPercentage();
			this->is_limit_set = true;
			break;
		case LimitNodeType::EXPRESSION_PERCENTAGE:
			this->is_limit_set = false;
			break;
		default:
			throw InternalException("Unsupported type for limit value in PhysicalLimitPercent");
		}
		switch (op.offset_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
			this->offset = op.offset_val.GetConstantValue();
			break;
		case LimitNodeType::UNSET:
			this->offset = 0;
			break;
		case LimitNodeType::EXPRESSION_VALUE:
			break;
		default:
			throw InternalException("Unsupported type for offset value in PhysicalLimitPercent");
		}
	}

	idx_t current_offset;
	double limit_percent;
	optional_idx offset;
	ColumnDataCollection data;

	bool is_limit_set = false;
};

unique_ptr<GlobalSinkState> PhysicalLimitPercent::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LimitPercentGlobalState>(context, *this);
}

SinkResultType PhysicalLimitPercent::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	D_ASSERT(chunk.size() > 0);
	auto &state = input.global_state.Cast<LimitPercentGlobalState>();
	auto &limit_percent = state.limit_percent;
	auto &offset = state.offset;

	// get the next chunk from the child
	if (!state.is_limit_set) {
		Value val = PhysicalLimit::GetDelimiter(context, chunk, limit_val.GetPercentageExpression());
		if (!val.IsNull()) {
			limit_percent = val.GetValue<double>();
		} else {
			limit_percent = 100.0;
		}
		if (limit_percent < 0.0) {
			throw BinderException("Percentage value(%f) can't be negative", limit_percent);
		}
		state.is_limit_set = true;
	}
	if (!state.offset.IsValid()) {
		Value val = PhysicalLimit::GetDelimiter(context, chunk, offset_val.GetValueExpression());
		if (!val.IsNull()) {
			offset = val.GetValue<idx_t>();
		} else {
			offset = 0;
		}
		if (offset.GetIndex() > 1ULL << 62ULL) {
			throw BinderException("Max value %lld for LIMIT/OFFSET is %lld", offset.GetIndex(), 1ULL << 62ULL);
		}
	}

	if (!PhysicalLimit::HandleOffset(chunk, state.current_offset, offset.GetIndex(), NumericLimits<idx_t>::Maximum())) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	state.data.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class LimitPercentOperatorState : public GlobalSourceState {
public:
	explicit LimitPercentOperatorState(const PhysicalLimitPercent &op) : current_offset(0) {
		D_ASSERT(op.sink_state);
		auto &gstate = op.sink_state->Cast<LimitPercentGlobalState>();
		gstate.data.InitializeScan(scan_state);
	}

	ColumnDataScanState scan_state;
	optional_idx limit;
	idx_t current_offset;
};

unique_ptr<GlobalSourceState> PhysicalLimitPercent::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<LimitPercentOperatorState>(*this);
}

SourceResultType PhysicalLimitPercent::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<LimitPercentGlobalState>();
	auto &state = input.global_state.Cast<LimitPercentOperatorState>();
	auto &percent_limit = gstate.limit_percent;
	auto &offset = gstate.offset;
	auto &limit = state.limit;
	auto &current_offset = state.current_offset;

	if (!limit.IsValid()) {
		if (!gstate.is_limit_set) {
			// no limit value and we have not set limit_percent
			// we are running LIMIT % with a subquery over an empty table
			D_ASSERT(gstate.data.Count() == 0);
			return SourceResultType::FINISHED;
		}
		idx_t count = gstate.data.Count();
		if (count > 0) {
			count += offset.GetIndex();
		}
		if (Value::IsNan(percent_limit) || percent_limit < 0 || percent_limit > 100) {
			throw OutOfRangeException("Limit percent out of range, should be between 0% and 100%");
		}
		auto limit_percentage = idx_t(percent_limit / 100.0 * double(count));
		if (limit_percentage > count) {
			limit = count;
		} else {
			limit = idx_t(limit_percentage);
		}
		if (limit == 0) {
			return SourceResultType::FINISHED;
		}
	}

	if (current_offset >= limit.GetIndex()) {
		return SourceResultType::FINISHED;
	}
	if (!gstate.data.Scan(state.scan_state, chunk)) {
		return SourceResultType::FINISHED;
	}

	PhysicalLimit::HandleOffset(chunk, current_offset, 0, limit.GetIndex());

	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
