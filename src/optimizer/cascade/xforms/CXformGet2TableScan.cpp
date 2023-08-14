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
	tmp_function.bind_replace = operator_get->function.bind_replace;
	tmp_function.in_out_function = operator_get->function.in_out_function;
	tmp_function.in_out_function_final = operator_get->function.in_out_function_final;
	tmp_function.statistics = operator_get->function.statistics;
	tmp_function.dependency = operator_get->function.dependency;
	tmp_function.cardinality = operator_get->function.cardinality;
	tmp_function.pushdown_complex_filter = operator_get->function.pushdown_complex_filter;
	tmp_function.to_string = operator_get->function.to_string;
	tmp_function.table_scan_progress = operator_get->function.table_scan_progress;
	tmp_function.get_batch_index = operator_get->function.get_batch_index;
	tmp_function.get_batch_info = operator_get->function.get_batch_info;
	tmp_function.serialize = operator_get->function.serialize;
	tmp_function.deserialize = operator_get->function.deserialize;
	tmp_function.projection_pushdown = operator_get->function.projection_pushdown;
	tmp_function.filter_pushdown = operator_get->function.filter_pushdown;
	tmp_function.filter_prune = operator_get->function.filter_prune;
	tmp_function.function_info = operator_get->function.function_info;
	duckdb::unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(operator_get->bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = operator_get->bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	if(operator_get->table_filters.filters.size() > 0) {
		table_filters = make_uniq<TableFilterSet>();
		for(auto &child : operator_get->table_filters.filters) {
			table_filters->filters.insert(make_pair(child.first, child.second->Copy()));
		}
	}
	// create alternative expression
	duckdb::unique_ptr<Operator> alternative_expression =
	    make_uniq<PhysicalTableScan>(operator_get->returned_types, operator_get->function, std::move(tmp_bind_data),
	                                 operator_get->column_ids, operator_get->names, std::move(table_filters), 1);
	// add alternative to transformation result
	xform_result->Add(std::move(alternative_expression));
}
} // namespace gpopt