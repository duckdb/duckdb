//
// Created by admin on 8/13/23.
//

#include "duckdb/optimizer/cascade/xforms/CXformDummyScanImplementation.h"

#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace gpopt {
CXform::EXformPromise CXformDummyScanImplementation::XformPromise(CExpressionHandle &expression_handle) const {
	return CXform::ExfpHigh;
}

void CXformDummyScanImplementation::Transform(CXformContext *xform_context, CXformResult *xform_result,
                                              Operator *expression) const {
	LogicalDummyScan *logical_dummy_scan = static_cast<LogicalDummyScan *>(expression);
	duckdb::unique_ptr<PhysicalDummyScan> alternative_expression =
	    make_uniq<PhysicalDummyScan>(logical_dummy_scan->types, logical_dummy_scan->estimated_cardinality);
	alternative_expression->v_column_binding = logical_dummy_scan->GetColumnBindings();
	xform_result->Add(std::move(alternative_expression));
}
} // namespace gpopt
