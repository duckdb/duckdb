#include "duckdb/common/sorting/sort.hpp"

#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

Sort::Sort(const vector<BoundOrderByNode> &orders, const vector<LogicalType> &types, vector<idx_t> projection_map) {
	// Initialize key expressions
	key_expressions.reserve(orders.size());
	for (auto &order : orders) {
		key_expressions.push_back(order.expression->Copy());
	}

	// For convenience, we fill the projection map if it is empty
	if (projection_map.empty()) {
		projection_map.reserve(types.size());
		for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
			projection_map.push_back(col_idx);
		}
	}

	// We need to output this many columns, reserve
	output_projection_columns.reserve(projection_map.size());

	// Create mapping from input column to key (so we won't duplicate columns in key/payload)
	unordered_map<idx_t, idx_t> input_column_to_key;
	for (idx_t key_idx = 0; key_idx < orders.size(); key_idx++) {
		const auto &key_order_expr = *orders[key_idx].expression;
		if (key_order_expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			input_column_to_key.emplace(key_order_expr.Cast<BoundReferenceExpression>().index, key_idx);
		}
	}

	// Construct payload layout (excluding columns that also appear as key)
	vector<LogicalType> payload_types;
	for (idx_t output_col_idx = 0; output_col_idx < projection_map.size(); output_col_idx++) {
		const auto &input_col_idx = projection_map[output_col_idx];
		const auto it = input_column_to_key.find(input_col_idx);
		if (it != input_column_to_key.end()) {
			// Projected column also appears as a key, just reference it
			output_projection_columns.push_back({false, it->second, output_col_idx});
		} else {
			// Projected column does not appear as a key, add to payload layout
			output_projection_columns.push_back({true, payload_types.size(), output_col_idx});
			payload_types.push_back(types[input_col_idx]);
			input_projection_map.push_back(input_col_idx);
		}
	}
	payload_layout.Initialize(payload_types);

	// Sort the output projection columns so we're gathering the columns in order
	std::sort(output_projection_columns.begin(), output_projection_columns.end(),
	          [](const SortProjectionColumn &lhs, const SortProjectionColumn &rhs) {
		          if (lhs.is_payload == rhs.is_payload) {
			          return lhs.layout_col_idx < rhs.layout_col_idx;
		          }
		          return lhs.is_payload < rhs.is_payload;
	          });

	// Finally, initialize the key layout (now that we know whether we have a payload)
	key_layout.Initialize(orders, !payload_types.empty());
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class SortLocalSinkState : public LocalSinkState {
public:
	SortLocalSinkState(const Sort &sort, ClientContext &context) {
		throw NotImplementedException("Sort");
	}
};

class SortGlobalSinkState : public GlobalSinkState {
public:
	SortGlobalSinkState(const Sort &sort, ClientContext &context) {
		throw NotImplementedException("Sort");
	}
};

unique_ptr<LocalSinkState> Sort::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<SortLocalSinkState>(*this, context.client);
}

unique_ptr<GlobalSinkState> Sort::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SortGlobalSinkState>(*this, context);
}

SinkResultType Sort::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<SortGlobalSinkState>();
	auto &lstate = input.local_state.Cast<SortGlobalSinkState>();

	// TODO: Keep a "Value" min/max per sorted run:
	//  1. Allows runs to be concatenated without merging
	//  2. Can identify sets of runs that overlap within the set, but the sets might not overlap with another set
	//    * For example, this could reduce one 100-ary merge into five 20-ary merges
	//    * This is probably going to be a really complicated algorithm (lots of trade-offs)
	//  3. Need C++ iterator over fixed-size blocks, use FastMod to reduce cost of modulo tuples per block

	throw NotImplementedException("Sort");

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType Sort::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<SortGlobalSinkState>();
	auto &lstate = input.local_state.Cast<SortGlobalSinkState>();

	throw NotImplementedException("Sort");

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType Sort::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<SortGlobalSinkState>();

	throw NotImplementedException("Sort");

	// if empty return SinkFinalizeType::NO_OUTPUT_POSSIBLE
	return SinkFinalizeType::READY;
}

ProgressData Sort::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate_p,
                                   const ProgressData source_progress) const {
	auto &gstate = gstate_p.Cast<SortGlobalSinkState>();

	throw NotImplementedException("Sort");

	return source_progress;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class SortGlobalSourceState : public GlobalSourceState {
public:
	SortGlobalSourceState(const Sort &sort, ClientContext &context, SortGlobalSinkState &sink_p) : sink(sink_p) {
	}

	idx_t MaxThreads() override {
		throw NotImplementedException("Sort");
	}

	SortGlobalSinkState &sink;
};

class SortLocalSourceState : public LocalSourceState {
public:
	SortLocalSourceState(const Sort &sort, ClientContext &context, SortGlobalSourceState &gstate) {
		throw NotImplementedException("Sort");
	}
};

unique_ptr<LocalSourceState> Sort::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
	return make_uniq<SortLocalSourceState>(*this, context.client, gstate.Cast<SortGlobalSourceState>());
}

unique_ptr<GlobalSourceState> Sort::GetGlobalSourceState(ClientContext &context, GlobalSinkState &sink) const {
	return make_uniq<SortGlobalSourceState>(*this, context, sink.Cast<SortGlobalSinkState>());
}

SourceResultType Sort::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<SortGlobalSourceState>();
	auto &lstate = input.local_state.Cast<SortGlobalSourceState>();
	// return SourceResultType::BLOCKED as needed

	throw NotImplementedException("Sort");

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

ProgressData Sort::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	throw NotImplementedException("Sort");
}

OperatorPartitionData Sort::GetPartitionData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                             LocalSourceState &lstate,
                                             const OperatorPartitionInfo &partition_info) const {
	throw NotImplementedException("Sort");
}

} // namespace duckdb
