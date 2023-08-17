//---------------------------------------------------------------------------
//	@filename:
//		CXformJoinCommutativity.h
//
//	@doc:
//		Join Commutativity, A Join B = B Join A
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
//		CXformJoinCommutativity
//
//	@doc:
//		Commute the join order
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