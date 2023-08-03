//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformGet2TableScan.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::CXformGet2TableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGet2TableScan::CXformGet2TableScan()
	: CXformImplementation(make_uniq<LogicalGet>(0, TableFunction(), nullptr, duckdb::vector<LogicalType>(), duckdb::vector<string>()))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformGet2TableScan::Exfp(CExpressionHandle &exprhdl) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformGet2TableScan::Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const
{
	LogicalGet* popGet = (LogicalGet*)pexpr;
	// create/extract components for alternative
	duckdb::vector<ColumnBinding> pdrgpcrOutput = popGet->GetColumnBindings();
	TableFunction tmp_function(popGet->function.name, popGet->function.arguments, popGet->function.function, popGet->function.bind, popGet->function.init_global, popGet->function.init_local);
	// unique_ptr<TableFunctionData> tmp_bind_data = make_uniq<TableFunctionData>();
	duckdb::unique_ptr<TableScanBindData> tmp_bind_data = make_uniq<TableScanBindData>(popGet->bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = popGet->bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	// create alternative expression
	duckdb::unique_ptr<Operator> pexprAlt = make_uniq<PhysicalTableScan>(popGet->returned_types, popGet->function, std::move(tmp_bind_data), popGet->column_ids, popGet->names, std::move(table_filters), 1);
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}