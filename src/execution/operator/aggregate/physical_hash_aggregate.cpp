#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/partitionable_hashtable.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions, idx_t estimated_cardinality,
                                             PhysicalOperatorType type)
    : PhysicalHashAggregate(context, move(types), move(expressions), {}, estimated_cardinality, type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, idx_t estimated_cardinality,
                                             PhysicalOperatorType type)
    : PhysicalHashAggregate(context, move(types), move(expressions), move(groups_p), {}, {}, estimated_cardinality,
                            type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p,
                                             vector<GroupingSet> grouping_sets_p,
                                             vector<vector<idx_t>> grouping_functions_p, idx_t estimated_cardinality,
                                             PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), grouping_sets(move(grouping_sets_p)) {
	// get a list of all aggregates to be computed
	const idx_t group_count = groups_p.size();
	if (grouping_sets.empty()) {
		GroupingSet set;
		for (idx_t i = 0; i < group_count; i++) {
			set.insert(i);
		}
		grouping_sets.push_back(move(set));
	}
	grouped_aggregate_data.InitializeGroupby(move(groups_p), move(expressions), move(grouping_functions_p));

	auto &aggregates = grouped_aggregate_data.aggregates;
	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		aggregate_input_idx += aggr.children.size();
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
			auto it = filter_indexes.find(aggr.filter.get());
			if (it == filter_indexes.end()) {
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}

	for (auto &grouping_set : grouping_sets) {
		radix_tables.emplace_back(grouping_set, grouped_aggregate_data);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalState : public GlobalSinkState {
public:
	HashAggregateGlobalState(const PhysicalHashAggregate &op, ClientContext &context) {
		radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSinkState(context));
		}
	}

	vector<unique_ptr<GlobalSinkState>> radix_states;
};

class HashAggregateLocalState : public LocalSinkState {
public:
	HashAggregateLocalState(const PhysicalHashAggregate &op, ExecutionContext &context) {
		auto &payload_types = op.grouped_aggregate_data.payload_types;
		if (!payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(payload_types);
		}

		radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetLocalSinkState(context));
		}
	}

	DataChunk aggregate_input_chunk;

	vector<unique_ptr<LocalSinkState>> radix_states;
};

void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = (HashAggregateGlobalState &)state;
	for (auto &radix_state : gstate.radix_states) {
		RadixPartitionedHashTable::SetMultiScan(*radix_state);
	}
}

unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<HashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<HashAggregateLocalState>(*this, context);
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                           DataChunk &input) const {
	auto &llstate = (HashAggregateLocalState &)lstate;
	auto &gstate = (HashAggregateGlobalState &)state;

	DataChunk &aggregate_input_chunk = llstate.aggregate_input_chunk;

	auto &aggregates = grouped_aggregate_data.aggregates;
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			D_ASSERT(bound_ref_expr.index < input.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < input.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[it->second]);
		}
	}

	aggregate_input_chunk.SetCardinality(input.size());
	aggregate_input_chunk.Verify();

	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Sink(context, *gstate.radix_states[i], *llstate.radix_states[i], input, aggregate_input_chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashAggregate::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const {
	auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalState &)lstate;

	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Combine(context, *gstate.radix_states[i], *llstate.radix_states[i]);
	}
}

class HashAggregateFinalizeEvent : public BasePipelineEvent {
public:
	HashAggregateFinalizeEvent(const PhysicalHashAggregate &op_p, HashAggregateGlobalState &gstate_p,
	                           Pipeline *pipeline_p)
	    : BasePipelineEvent(*pipeline_p), op(op_p), gstate(gstate_p) {
	}

	const PhysicalHashAggregate &op;
	HashAggregateGlobalState &gstate;

public:
	void Schedule() override {
		vector<unique_ptr<Task>> tasks;
		for (idx_t i = 0; i < op.radix_tables.size(); i++) {
			op.radix_tables[i].ScheduleTasks(pipeline->executor, shared_from_this(), *gstate.radix_states[i], tasks);
		}
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 GlobalSinkState &gstate_p) const {
	auto &gstate = (HashAggregateGlobalState &)gstate_p;
	bool any_partitioned = false;
	for (idx_t i = 0; i < gstate.radix_states.size(); i++) {
		bool is_partitioned = radix_tables[i].Finalize(context, *gstate.radix_states[i]);
		if (is_partitioned) {
			any_partitioned = true;
		}
	}
	if (any_partitioned) {
		auto new_event = make_shared<HashAggregateFinalizeEvent>(*this, gstate, &pipeline);
		event.InsertEvent(move(new_event));
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PhysicalHashAggregateGlobalSourceState : public GlobalSourceState {
public:
	PhysicalHashAggregateGlobalSourceState(ClientContext &context, const PhysicalHashAggregate &op)
	    : op(op), state_index(0) {
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSourceState(context));
		}
	}

	const PhysicalHashAggregate &op;
	mutex lock;
	atomic<idx_t> state_index;

	vector<unique_ptr<GlobalSourceState>> radix_states;

public:
	idx_t MaxThreads() override {
		// If there are no tables, we only need one thread.
		if (op.radix_tables.empty()) {
			return 1;
		}

		auto &ht_state = (HashAggregateGlobalState &)*op.sink_state;
		idx_t count = 0;
		for (size_t sidx = 0; sidx < op.radix_tables.size(); ++sidx) {
			count += op.radix_tables[sidx].Size(*ht_state.radix_states[sidx]);
		}
		return MaxValue<idx_t>(1, count / RowGroup::ROW_GROUP_SIZE);
	}
};

unique_ptr<GlobalSourceState> PhysicalHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalHashAggregateGlobalSourceState>(context, *this);
}

class PhysicalHashAggregateLocalSourceState : public LocalSourceState {
public:
	explicit PhysicalHashAggregateLocalSourceState(ExecutionContext &context, const PhysicalHashAggregate &op) {
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetLocalSourceState(context));
		}
	}

	vector<unique_ptr<LocalSourceState>> radix_states;
};

unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceState(ExecutionContext &context,
                                                                        GlobalSourceState &gstate) const {
	return make_unique<PhysicalHashAggregateLocalSourceState>(context, *this);
}

void PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                    LocalSourceState &lstate_p) const {
	auto &ht_state = (HashAggregateGlobalState &)*sink_state;
	auto &gstate = (PhysicalHashAggregateGlobalSourceState &)gstate_p;
	auto &lstate = (PhysicalHashAggregateLocalSourceState &)lstate_p;
	while (true) {
		idx_t radix_idx = gstate.state_index;
		if (radix_idx >= radix_tables.size()) {
			break;
		}
		radix_tables[radix_idx].GetData(context, chunk, *ht_state.radix_states[radix_idx],
		                                *gstate.radix_states[radix_idx], *lstate.radix_states[radix_idx]);
		if (chunk.size() != 0) {
			return;
		}
		// move to the next table
		lock_guard<mutex> l(gstate.lock);
		radix_idx++;
		if (radix_idx > gstate.state_index) {
			// we have not yet worked on the table
			// move the global index forwards
			gstate.state_index = radix_idx;
		}
	}
}

string PhysicalHashAggregate::ParamsToString() const {
	string result;
	auto &groups = grouped_aggregate_data.groups;
	auto &aggregates = grouped_aggregate_data.aggregates;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb
