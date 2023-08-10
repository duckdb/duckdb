//---------------------------------------------------------------------------
//	@filename:
//		CXformFilterImplementation.h
//
//	@doc:
//		Transform Logical Projection to Physical Projection
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformFilterImplementation_H
#define GPOPT_CXformFilterImplementation_H

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
class CXformFilterImplementation : public CXformImplementation
{
public:
	// ctor
	explicit CXformFilterImplementation();
    
    CXformFilterImplementation(const CXformFilterImplementation &) = delete;
	
    // dtor
	virtual ~CXformFilterImplementation()
	{
	}

	// ident accessors
	virtual EXformId Exfid() const
	{
		return ExfFilterImplementation;
	}

	// return a string for xform name
	virtual const CHAR* SzId() const
	{
		return "CXformFilterImplementation";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const override;
};
}  // namespace gpopt
#endif