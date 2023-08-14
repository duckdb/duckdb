//
// @filename: CXformDummyScanImplementation
//
// @doc: Transform Dummy Scan to a physical operator
//

#pragma once

#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformImplementation.h"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace gpopt {

class CXformDummyScanImplementation : public CXformImplementation {
public:
	CXformDummyScanImplementation() : CXformImplementation(make_uniq<LogicalDummyScan>(0)) {};

	EXformId ID() const override {
		return ExfDummyScanImplementation;
	}

	const char *Name() const override {
		return "CXformDummyScanImplementation";
	}

	EXformPromise XformPromise(CExpressionHandle &expression_handle) const override;

	void Transform(CXformContext *xform_context, CXformResult *xform_result, Operator *expression) const override;

private:
	CXformDummyScanImplementation(const CXformDummyScanImplementation &);
}; // class CXformDummyScanImplementation
} // namespace gpopt
