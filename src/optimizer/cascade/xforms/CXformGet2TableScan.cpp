//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformGet2TableScan.h"

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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
	LogicalGet *op = static_cast<LogicalGet *>(expression);

	// copy bind data
	duckdb::unique_ptr<TableScanBindData> cpy_bind_data =
	    make_uniq<TableScanBindData>(op->bind_data->Cast<TableScanBindData>().table);
	cpy_bind_data->column_ids = op->bind_data->Cast<TableFunctionData>().column_ids;

	// transform table filters
	duckdb::unique_ptr<TableFilterSet> table_filters;
	if (!op->table_filters.filters.empty()) {
		table_filters = CreateTableFilterSet(op->table_filters, op->column_ids);
	}

	// create alternative expression
	duckdb::unique_ptr<PhysicalTableScan> alternative_expression = make_uniq<PhysicalTableScan>(
	    op->types, op->function, std::move(cpy_bind_data), op->returned_types, op->column_ids,
	    duckdb::vector<column_t>(), op->names, std::move(table_filters), 1);

	// column binding
	alternative_expression->v_column_binding = op->GetColumnBindings();

	// add alternative to transformation result
	xform_result->Add(std::move(alternative_expression));
}

duckdb::unique_ptr<TableFilterSet>
CXformGet2TableScan::CreateTableFilterSet(TableFilterSet &table_filters, duckdb::vector<column_t> &column_ids) const {
	// create the table filter map
	auto table_filter_set = make_uniq<TableFilterSet>();
	for (auto &table_filter : table_filters.filters) {
		// find the relative column index from the absolute column index into the table
		idx_t column_index = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (table_filter.first == column_ids[i]) {
				column_index = i;
				break;
			}
		}
		if (column_index == DConstants::INVALID_INDEX) {
			throw InternalException("Could not find column index for table filter");
		}
		table_filter_set->filters[column_index] = table_filter.second->Copy();
	}
	return table_filter_set;
}
} // namespace gpopt