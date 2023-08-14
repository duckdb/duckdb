//---------------------------------------------------------------------------
//	@filename:
//		CXformJoinCommutativity.h
//
//	@doc:
//		Transform Logical Projection to Physical Projection
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoinCommutativity_H
#define GPOPT_CXformJoinCommutativity_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformExploration.h"

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
class CXformJoinCommutativity : public CXformExploration
{
public:
	// ctor
	explicit CXformJoinCommutativity();
    
    CXformJoinCommutativity(const CXformJoinCommutativity &) = delete;
	
    // dtor
	virtual ~CXformJoinCommutativity()
	{
	}

	// ident accessors
	virtual EXformId ID() const
	{
		return ExfJoinCommutativity;
	}

	// return a string for xform name
	virtual const CHAR* Name() const
	{
		return "CXformJoinCommutativity";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise XformPromise(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const override;
};
}  // namespace gpopt
#endif