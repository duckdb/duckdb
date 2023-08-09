//---------------------------------------------------------------------------
//	@filename:
//		CXformOrderImplementation.h
//
//	@doc:
//		Transform Logical Projection to Physical Projection
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformOrderImplementation_H
#define GPOPT_CXformOrderImplementation_H

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
class CXformOrderImplementation : public CXformImplementation
{
public:
	// ctor
	explicit CXformOrderImplementation();
    
    CXformOrderImplementation(const CXformOrderImplementation &) = delete;
	
    // dtor
	virtual ~CXformOrderImplementation()
	{
	}

	// ident accessors
	virtual EXformId Exfid() const
	{
		return ExfOrderImplementation;
	}

	// return a string for xform name
	virtual const CHAR* SzId() const
	{
		return "CXformOrderImplementation";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const override;
};
}  // namespace gpopt
#endif