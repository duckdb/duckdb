#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/perfect_aggregate_hashtable.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

PhysicalPerfectHashAggregate::PhysicalPerfectHashAggregate(ClientContext &context, vector<LogicalType> types_p,
                                                           vector<unique_ptr<Expression>> aggregates_p,
                                                           vector<unique_ptr<Expression>> groups_p,
                                                           vector<unique_ptr<BaseStatistics>> group_stats,
                                                           vector<idx_t> required_bits_p)
    : PhysicalSink(PhysicalOperatorType::PERFECT_HASH_GROUP_BY, move(types_p)), groups(move(groups_p)),
      aggregates(move(aggregates_p)), required_bits(move(required_bits_p)) {
	D_ASSERT(groups.size() == group_stats.size());
	group_minima.reserve(group_stats.size());
	for (auto &stats : group_stats) {
		D_ASSERT(stats);
		auto &nstats = (NumericStatistics &)*stats;
		D_ASSERT(!nstats.min.is_null);
		group_minima.push_back(move(nstats.min));
	}
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}

	vector<BoundAggregateExpression *> bindings;
	for (auto &expr : aggregates) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = (BoundAggregateExpression &)*expr;
		bindings.push_back(&aggr);

		D_ASSERT(!aggr.distinct);
		D_ASSERT(aggr.function.combine);
		for (auto & child : aggr.children) {
			payload_types.push_back(child->return_type);
		}
		if (aggr.filter){
			vector<LogicalType> types;
			vector<vector<Expression*>> bound_refs;
			BoundAggregateExpression::GetColumnRef(aggr.filter.get(),bound_refs,types);
			for (auto type:types){
				payload_types.push_back(type);
			}
		}
	}
	aggregate_objects = AggregateObject::CreateAggregateObjects(move(bindings));
}

unique_ptr<PerfectAggregateHashTable> PhysicalPerfectHashAggregate::CreateHT(ClientContext &context) {
	return make_unique<PerfectAggregateHashTable>(BufferManager::GetBufferManager(context), group_types, payload_types,
	                                              aggregate_objects, group_minima, required_bits);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PerfectHashAggregateGlobalState : public GlobalOperatorState {
public:
	PerfectHashAggregateGlobalState(PhysicalPerfectHashAggregate &op, ClientContext &context)
	    : ht(op.CreateHT(context)) {
	}

	//! The lock for updating the global aggregate state
	std::mutex lock;
	//! The global aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
};

class PerfectHashAggregateLocalState : public LocalSinkState {
public:
	PerfectHashAggregateLocalState(PhysicalPerfectHashAggregate &op, ClientContext &context)
	    : ht(op.CreateHT(context)) {
		group_chunk.InitializeEmpty(op.group_types);
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}
	}

	//! The local aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
	DataChunk group_chunk;
	DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalOperatorState> PhysicalPerfectHashAggregate::GetGlobalState(ClientContext &context) {
	return make_unique<PerfectHashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalPerfectHashAggregate::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<PerfectHashAggregateLocalState>(*this, context.client);
}

void PhysicalPerfectHashAggregate::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                                        DataChunk &input) {
	auto &lstate = (PerfectHashAggregateLocalState &)lstate_p;
	DataChunk &group_chunk = lstate.group_chunk;
	DataChunk &aggregate_input_chunk = lstate.aggregate_input_chunk;

	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = (BoundReferenceExpression &)*group;
		group_chunk.data[group_idx].Reference(input.data[bound_ref_expr.index]);
	}
	idx_t aggregate_input_idx = 0;
	for (auto & aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
		if (aggr.filter){
			vector<LogicalType> types;
			vector<vector<Expression*>> bound_refs;
			BoundAggregateExpression::GetColumnRef(aggr.filter.get(),bound_refs,types);
			for (auto &bound_ref : bound_refs){
				auto &bound_ref_expr = (BoundReferenceExpression &)*bound_ref[0];
			    aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
				for (auto&bound_ref_up : bound_ref){
					auto &bound_ref_up_expr = (BoundReferenceExpression &)*bound_ref_up;
					bound_ref_up_expr.index = aggregate_input_idx-1;
				}
			}
		}
	}


	group_chunk.SetCardinality(input.size());

	aggregate_input_chunk.SetCardinality(input.size());

	group_chunk.Verify();
	aggregate_input_chunk.Verify();
	D_ASSERT(aggregate_input_chunk.ColumnCount() == 0 || group_chunk.size() == aggregate_input_chunk.size());

	lstate.ht->AddChunk(group_chunk, aggregate_input_chunk);
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalPerfectHashAggregate::Combine(ExecutionContext &context, GlobalOperatorState &gstate_p,
                                           LocalSinkState &lstate_p) {
	auto &lstate = (PerfectHashAggregateLocalState &)lstate_p;
	auto &gstate = (PerfectHashAggregateGlobalState &)gstate_p;

	lock_guard<mutex> l(gstate.lock);
	gstate.ht->Combine(*lstate.ht);
}

//===--------------------------------------------------------------------===//
// GetChunk
//===--------------------------------------------------------------------===//
class PerfectHashAggregateState : public PhysicalOperatorState {
public:
	PerfectHashAggregateState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), ht_scan_position(0) {
	}
	//! The current position to scan the HT for output tuples
	idx_t ht_scan_position;
};

void PhysicalPerfectHashAggregate::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                                    PhysicalOperatorState *state_p) {
	auto &state = (PerfectHashAggregateState &)*state_p;
	auto &gstate = (PerfectHashAggregateGlobalState &)*sink_state;

	gstate.ht->Scan(state.ht_scan_position, chunk);
}

unique_ptr<PhysicalOperatorState> PhysicalPerfectHashAggregate::GetOperatorState() {
	return make_unique<PerfectHashAggregateState>(*this, children[0].get());
}

string PhysicalPerfectHashAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];
		if (aggregate.filter){
			result += aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb
