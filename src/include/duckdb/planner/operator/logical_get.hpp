//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_get.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

//! LogicalGet represents a scan operation from a data source
class LogicalGet : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_GET;

public:
	LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
	           vector<LogicalType> returned_types, vector<string> returned_names);

	//! The table index in the current bind context
	idx_t table_index;
	//! The function that is called
	TableFunction function;
	//! The bind data of the function
	duckdb::unique_ptr<FunctionData> bind_data;
	//! The types of ALL columns that can be returned by the table function
	duckdb::vector<LogicalType> returned_types;
	//! The names of ALL columns that can be returned by the table function
	duckdb::vector<string> names;
	//! Bound column IDs
	duckdb::vector<column_t> column_ids;
	//! Columns that are used outside of the scan
	duckdb::vector<idx_t> projection_ids;
	//! Filters pushed down for table scan
	TableFilterSet table_filters;
	//! The set of input parameters for the table function
	duckdb::vector<Value> parameters;
	//! The set of named input parameters for the table function
	named_parameter_map_t named_parameters;
	//! The set of named input table types for the table-in table-out function
	duckdb::vector<LogicalType> input_table_types;
	//! The set of named input table names for the table-in table-out function
	duckdb::vector<string> input_table_names;
	//! For a table-in-out function, the set of projected input columns
	duckdb::vector<column_t> projected_input;

	string GetName() const override;
	string ParamsToString() const override;
	//! Returns the underlying table that is being scanned, or nullptr if there is none
	TableCatalogEntry *GetTable() const;

public:
	duckdb::vector<ColumnBinding> GetColumnBindings() override;

	idx_t EstimateCardinality(ClientContext &context) override;

	void Serialize(FieldWriter &writer) const override;

	static duckdb::unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

	vector<idx_t> GetTableIndex() const override;

protected:
	void ResolveTypes() override;

public:
	// ----------------- ORCA -------------------------
	ULONG HashValue() const override;

	// derive join depth
	ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) override {
		return 1;
	}

	CXform_set *PxfsCandidates() const override;

	CPropConstraint *DerivePropertyConstraint(CExpressionHandle &exprhdl) override;

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
