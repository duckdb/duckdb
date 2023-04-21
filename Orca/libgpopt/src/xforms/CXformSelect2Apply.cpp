//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSelect2Apply.cpp
//
//	@doc:
//		Implementation of Select to Apply transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSelect2Apply.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2Apply::CXformSelect2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2Apply::CXformSelect2Apply(CMemoryPool *mp)
	:  // pattern
	  CXformSubqueryUnnest(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
		  ))
{
}

// EOF
