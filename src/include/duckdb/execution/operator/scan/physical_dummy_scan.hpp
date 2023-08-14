//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_dummy_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"

namespace duckdb {

class PhysicalDummyScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::DUMMY_SCAN;

public:
	explicit PhysicalDummyScan(vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN, std::move(types), estimated_cardinality) {
		m_derived_property_relation = new gpopt::CDrvdPropRelational();
		m_group_expression = nullptr;
		m_derived_property_plan = nullptr;
		m_required_plan_property = nullptr;
		m_total_opt_requests = 1;
	}

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// ------------------------------ ORCA ---------------------------------

	ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) override;

	// Rehydrate expression from a given cost context and child expressions
	Operator *SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
	                        CDrvdPropCtxtPlan *pdpctxtplan) override;

	duckdb::unique_ptr<Operator> Copy() override;

	duckdb::unique_ptr<Operator> CopyWithNewGroupExpression(CGroupExpression *pgexpr) override;

	duckdb::unique_ptr<Operator> CopyWithNewChildren(CGroupExpression *pgexpr,
	                                                 duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
	                                                 double cost) override;
};
} // namespace duckdb
