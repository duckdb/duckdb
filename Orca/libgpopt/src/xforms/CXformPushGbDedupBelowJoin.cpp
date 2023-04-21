//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformPushGbDedupBelowJoin.cpp
//
//	@doc:
//		Implementation of pushing dedup group by below join transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformPushGbDedupBelowJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbDedupBelowJoin::CXformPushGbDedupBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbDedupBelowJoin::CXformPushGbDedupBelowJoin(CMemoryPool *mp)
	:  // pattern
	  CXformPushGbBelowJoin(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalGbAggDeduplicate(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // join outer child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // join inner child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // join predicate
			  ),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
		  ))
{
}

// EOF
