#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

#include "duckdb/optimizer/cascade/base/CCostContext.h"

namespace duckdb {

class DummyScanState : public GlobalSourceState {
public:
	DummyScanState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDummyScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DummyScanState>();
}

void PhysicalDummyScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                LocalSourceState &lstate) const {
	auto &state = (DummyScanState &)gstate;
	if (state.finished) {
		return;
	}
	// return a single row on the first call to the dummy scan
	chunk.SetCardinality(1);
	state.finished = true;
}

ULONG PhysicalDummyScan::DeriveJoinDepth(CExpressionHandle &exprhdl) {
	return 1;
}

Operator *PhysicalDummyScan::SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
                                           CDrvdPropCtxtPlan *pdpctxtplan) {
	PhysicalDummyScan *expr = new PhysicalDummyScan(types, estimated_cardinality);
	expr->m_cost = pcc->m_cost;
	expr->m_group_expression = pcc->m_group_expression;
	return expr;
}

duckdb::unique_ptr<Operator> PhysicalDummyScan::Copy() {
	return make_uniq<PhysicalDummyScan>(types, estimated_cardinality);
}

duckdb::unique_ptr<Operator> PhysicalDummyScan::CopyWithNewGroupExpression(CGroupExpression *pgexpr) {
	auto copy = Copy();
	copy->m_group_expression = pgexpr;
	return copy;
}

duckdb::unique_ptr<Operator>
PhysicalDummyScan::CopyWithNewChildren(CGroupExpression *pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                       double cost) {
	auto copy = Copy();
	for (auto &child : pdrgpexpr) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = pgexpr;
	copy->m_cost = cost;
	return copy;
}

} // namespace duckdb
