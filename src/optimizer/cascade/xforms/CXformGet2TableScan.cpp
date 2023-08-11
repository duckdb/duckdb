//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformGet2TableScan.h"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_get.hpp"

namespace gpopt {

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::CXformGet2TableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGet2TableScan::CXformGet2TableScan()
    : CXformImplementation(
          make_uniq<LogicalGet>(0, TableFunction(), nullptr, duckdb::vector<LogicalType>(), duckdb::vector<string>())) {
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::XformPromise
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformGet2TableScan::XformPromise(CExpressionHandle &expression_handle) const {
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
void CXformGet2TableScan::Transform(CXformContext *xform_context, CXformResult *xform_result,
                                    Operator *expression) const {
	LogicalGet *operator_get = static_cast<LogicalGet *>(expression);
	// create/extract components for alternative
	duckdb::vector<ColumnBinding> pdrgpcr_output = operator_get->GetColumnBindings();
	TableFunction tmp_function(operator_get->function.name, operator_get->function.arguments,
	                           operator_get->function.function, operator_get->function.bind,
	                           operator_get->function.init_global, operator_get->function.init_local);
	// unique_ptr<TableFunctionData> tmp_bind_data = make_uniq<TableFunctionData>();
	duckdb::unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(operator_get->bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = operator_get->bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	// create alternative expression
	duckdb::unique_ptr<Operator> alternative_expression =
	    make_uniq<PhysicalTableScan>(operator_get->returned_types, operator_get->function, std::move(tmp_bind_data),
	                                 operator_get->column_ids, operator_get->names, std::move(table_filters), 1);
	// add alternative to transformation result
	xform_result->Add(std::move(alternative_expression));
}
} // namespace gpopt