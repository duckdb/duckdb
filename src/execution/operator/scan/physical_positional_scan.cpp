#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <utility>

namespace duckdb {

PhysicalPositionalScan::PhysicalPositionalScan(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
                                               unique_ptr<PhysicalOperator> right)
    : PhysicalOperator(PhysicalOperatorType::POSITIONAL_SCAN, move(types),
                       MinValue(left->estimated_cardinality, right->estimated_cardinality)) {

	// Manage the children ourselves
	D_ASSERT(left->type == PhysicalOperatorType::TABLE_SCAN);
	D_ASSERT(right->type == PhysicalOperatorType::TABLE_SCAN);
	child_tables.emplace_back(move(left));
	child_tables.emplace_back(move(right));
}

class PositionalScanGlobalSourceState : public GlobalSourceState {
public:
	PositionalScanGlobalSourceState(ClientContext &context, const PhysicalPositionalScan &op) {
		for (const auto &table : op.child_tables) {
			global_states.emplace_back(table->GetGlobalSourceState(context));
		}
	}

	vector<unique_ptr<GlobalSourceState>> global_states;

	idx_t MaxThreads() override {
		return 1;
	}
};

class PositionalScanLocalSourceState : public LocalSourceState {
public:
	PositionalScanLocalSourceState(ExecutionContext &context, PositionalScanGlobalSourceState &gstate,
	                               const PhysicalPositionalScan &op) {
		for (size_t i = 0; i < op.child_tables.size(); ++i) {
			const auto &child = op.child_tables[i];
			auto &global_state = *gstate.global_states[i];
			local_states.emplace_back(child->GetLocalSourceState(context, global_state));
		}
	}

	vector<unique_ptr<LocalSourceState>> local_states;
};

unique_ptr<LocalSourceState> PhysicalPositionalScan::GetLocalSourceState(ExecutionContext &context,
                                                                         GlobalSourceState &gstate) const {
	return make_unique<PositionalScanLocalSourceState>(context, (PositionalScanGlobalSourceState &)gstate, *this);
}

unique_ptr<GlobalSourceState> PhysicalPositionalScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PositionalScanGlobalSourceState>(context, *this);
}

void PhysicalPositionalScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                     LocalSourceState &lstate_p) const {
	auto &gstate = (PositionalScanGlobalSourceState &)gstate_p;
	auto &lstate = (PositionalScanLocalSourceState &)lstate_p;

	auto &left = *child_tables[0];
	auto &right = *child_tables[1];
	DataChunk right_chunk;
	chunk.Split(right_chunk, left.types.size());

	left.GetData(context, chunk, *gstate.global_states[0], *lstate.local_states[0]);
	right.GetData(context, right_chunk, *gstate.global_states[1], *lstate.local_states[1]);

	const auto count = MinValue(chunk.size(), right_chunk.size());
	chunk.SetCardinality(count);
	right_chunk.SetCardinality(count);

	chunk.Fuse(right_chunk);
}

double PhysicalPositionalScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = (PositionalScanGlobalSourceState &)gstate_p;
	return MaxValue(child_tables[0]->GetProgress(context, *gstate.global_states[0]),
	                child_tables[1]->GetProgress(context, *gstate.global_states[1]));
}

string PhysicalPositionalScan::GetName() const {
	return child_tables[0]->GetName() + " " + PhysicalOperatorToString(type) + " " + child_tables[1]->GetName();
}

string PhysicalPositionalScan::ParamsToString() const {
	return child_tables[0]->ParamsToString() + "\n" + child_tables[1]->ParamsToString();
}

bool PhysicalPositionalScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}

	auto &other = (PhysicalPositionalScan &)other_p;
	if (child_tables.size() != other.child_tables.size()) {
		return false;
	}
	for (size_t i = 0; i < child_tables.size(); ++i) {
		if (!child_tables[i]->Equals(*other.child_tables[i])) {
			return false;
		}
	}

	return true;
}

} // namespace duckdb
