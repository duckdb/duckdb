//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.h
//
//	@doc:
//		Transform Get to TableScan
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformImplementation.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGet2TableScan
//
//	@doc:
//		Transform Get to TableScan
//
//---------------------------------------------------------------------------
class CXformGet2TableScan : public CXformImplementation {
public:
	// ctor
	CXformGet2TableScan();
	// dtor
	~CXformGet2TableScan() override = default;
	// ident accessors
	EXformId ID() const override {
		return ExfGet2TableScan;
	}
	// return a string for xform name
	const CHAR *Name() const override {
		return "CXformGet2TableScan";
	}
	// compute xform promise for a given expression handle
	EXformPromise XformPromise(CExpressionHandle &expression_handle) const override;
	// actual transform
	void Transform(CXformContext *xform_context, CXformResult *xform_result, Operator *expression) const override;

private:
	// private copy ctor
	CXformGet2TableScan(const CXformGet2TableScan &);
    // transform duckdb-style column_ids into orca-style.
	duckdb::unique_ptr<TableFilterSet> CreateTableFilterSet(TableFilterSet &table_filters,
	                                                        duckdb::vector<column_t> &column_ids) const;
};
} // namespace gpopt