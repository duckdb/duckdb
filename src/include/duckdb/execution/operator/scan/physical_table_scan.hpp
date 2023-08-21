//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_table_scan.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"

namespace gpopt {
class CEnfdOrder;
}

namespace duckdb {
using namespace gpopt;

//! Represents a scan of a base table
class PhysicalTableScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TABLE_SCAN;

public:
	//! Regular Table Scan
	PhysicalTableScan(vector<LogicalType> types, TableFunction function, unique_ptr<FunctionData> bind_data,
	                  vector<column_t> column_ids, vector<string> names, unique_ptr<TableFilterSet> table_filters,
	                  idx_t estimated_cardinality);
	//! Table scan that immediately projects out filter columns that are unused in the remainder of the query plan
	PhysicalTableScan(vector<LogicalType> types, TableFunction function, unique_ptr<FunctionData> bind_data,
	                  vector<LogicalType> returned_types, vector<column_t> column_ids, vector<idx_t> projection_ids,
	                  vector<string> names, unique_ptr<TableFilterSet> table_filters, idx_t estimated_cardinality);

	//! The table function
	TableFunction function;
	//! Bind data of the function
	unique_ptr<FunctionData> bind_data;
	//! The types of ALL columns that can be returned by the table function
	vector<LogicalType> returned_types;
	//! The column ids used within the table function
	vector<column_t> column_ids;
	//! The projected-out column ids
	vector<idx_t> projection_ids;
	//! The names of the columns
	vector<string> names;
	//! The table filters
	unique_ptr<TableFilterSet> table_filters;
	
	vector<ColumnBinding> v_column_binding;

public:
	string GetName() const override;
	string ParamsToString() const override;

	bool Equals(const PhysicalOperator &other) const override;

public:
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
	idx_t GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                    LocalSourceState &lstate) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	bool SupportsBatchIndex() const override {
		return function.get_batch_index != nullptr;
	}

	double GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;

public:
	ULONG HashValue() const override;
	
	CEnfdOrder::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl, vector<BoundOrderByNode> &peo) const override;

	COrderSpec* PosRequired(CExpressionHandle &exprhdl, gpopt::COrderSpec* posRequired,
							ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt,
							ULONG ulOptReq) const override {
		return nullptr;
	}

	// derive sort order
	COrderSpec*
	PosDerive(CExpressionHandle &exprhdl) const override
	{
		// return empty sort order
		return new COrderSpec();
	}

	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrsRequired,
	                       ULONG ulOptReq) const override;

	CKeyCollection *DeriveKeyCollection(CExpressionHandle &exprhdl) override;

	CPropConstraint *DerivePropertyConstraint(CExpressionHandle &exprhdl) override;

	ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) override;

	// Rehydrate expression from a given cost context and child expressions
	Operator *SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
	                        CDrvdPropCtxtPlan *pdpctxtplan) override;

	duckdb::unique_ptr<Operator> Copy() override;

	duckdb::unique_ptr<Operator> CopyWithNewGroupExpression(CGroupExpression *pgexpr) override;

	duckdb::unique_ptr<Operator> CopyWithNewChildren(CGroupExpression *pgexpr,
	                                                 duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
	                                                 double cost) override;

	vector<ColumnBinding> GetColumnBindings() override {
		return v_column_binding;
	}
};

} // namespace duckdb
