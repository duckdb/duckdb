//---------------------------------------------------------------------------
//	@filename:
//		CXformInnerJoin2HashJoin.h
//
//	@doc:
//		Transform Logical Projection to Physical Projection
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2HashJoin_H
#define GPOPT_CXformInnerJoin2HashJoin_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGet2TableScan
//
//	@doc:
//		Transform Get to TableScan
//
//---------------------------------------------------------------------------
class CXformInnerJoin2HashJoin : public CXformImplementation
{
public:
	// ctor
	explicit CXformInnerJoin2HashJoin();
    
    CXformInnerJoin2HashJoin(const CXformInnerJoin2HashJoin &) = delete;
	
    // dtor
	virtual ~CXformInnerJoin2HashJoin()
	{
	}

	// ident accessors
	virtual EXformId ID() const
	{
		return ExfInnerJoin2HashJoin;
	}

	// return a string for xform name
	virtual const CHAR* Name() const
	{
		return "CXformInnerJoin2HashJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise XformPromise(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const override;
};
}  // namespace gpopt
#endif