//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb
{
using namespace gpopt;

class OrderGlobalSinkState;

//! Physically re-orders the input data
class PhysicalOrder : public PhysicalOperator
{
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::ORDER_BY;

public:
	PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, vector<idx_t> projections, idx_t estimated_cardinality);

	//! Input data
	vector<BoundOrderByNode> orders;

	vector<idx_t> projections;

public:
	CEnfdOrder::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, vector<BoundOrderByNode> &peo) const override;
	
	COrderSpec* PosRequired(CExpressionHandle &exprhdl, gpopt::COrderSpec* posRequired, ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt, ULONG ulOptReq) const override;
	
	bool FProvidesReqdCols(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrsRequired, ULONG ulOptReq) const override;
	
	COrderSpec* PosDerive(gpopt::CExpressionHandle& exprhdl) const override
	{
		COrderSpec* result = new COrderSpec();
		for(auto &child: orders)
		{
			result->Append(child.type, child.null_order, child.expression.get());
		}
		return result;
	}

	vector<ColumnBinding> PcrsRequired(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrsRequired, ULONG child_index, vector<CDrvdProp*> pdrgpdpCtxt, ULONG ulOptReq) override;
	
	CKeyCollection* DeriveKeyCollection(CExpressionHandle &exprhdl) override
	{
		return NULL;
	}

	CPropConstraint* DerivePropertyConstraint(CExpressionHandle &exprhdl) override
	{
		return NULL;
	}

	// Rehydrate expression from a given cost context and child expressions
	Operator* SelfRehydrate(CCostContext* pcc, duckdb::vector<Operator*> pdrgpexpr, CDrvdPropCtxtPlan* pdpctxtplan) override;

	unique_ptr<Operator> Copy() override;

	unique_ptr<Operator> CopyWithNewGroupExpression(CGroupExpression* pgexpr) override;

	unique_ptr<Operator> CopyWithNewChildren(CGroupExpression* pgexpr, vector<unique_ptr<Operator>> pdrgpexpr,
											double cost) override;
public:
	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const override;
	
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate, LocalSourceState &lstate) const override;
	
	idx_t GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate, LocalSourceState &lstate) const override;

	bool IsSource() const override {
		return true;
	}

	bool ParallelSource() const override {
		return true;
	}

	bool SupportsBatchIndex() const override {
		return true;
	}

	OrderPreservationType SourceOrder() const override
	{
		return OrderPreservationType::FIXED_ORDER;
	}

public:
	// Sink interface
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p, DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context, GlobalSinkState &gstate) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	bool SinkOrderDependent() const override {
		return false;
	}

public:
	string ParamsToString() const override;

	//! Schedules tasks to merge the data during the Finalize phase
	static void ScheduleMergeTasks(Pipeline &pipeline, Event &event, OrderGlobalSinkState &state);

public:
	COrderSpec* Pos()
	{
		COrderSpec* result = new COrderSpec();
		for(auto &child: orders)
		{
			result->m_pdrgpoe.emplace_back(child.Copy());
		}
		return result;
	}
};

} // namespace duckdb
