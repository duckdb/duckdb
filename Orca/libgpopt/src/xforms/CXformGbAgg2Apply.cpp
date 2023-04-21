//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformGbAgg2Apply.cpp
//
//	@doc:
//		Implementation of GbAgg to Apply transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformGbAgg2Apply.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Apply::CXformGbAgg2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2Apply::CXformGbAgg2Apply(CMemoryPool *mp)
	:  // pattern
	  CXformSubqueryUnnest(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // project list
		  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Apply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		scalar child must have subquery
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2Apply::Exfp(CExpressionHandle &exprhdl) const
{
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (popGbAgg->FGlobal() && exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


// EOF
