//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoinNotIn2CrossProduct.h
//
//	@doc:
//		Transform left anti semi join with NotIn semantics to cross product
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoinNotIn2CrossProduct_H
#define GPOPT_CXformLeftAntiSemiJoinNotIn2CrossProduct_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/xforms/CXformLeftAntiSemiJoin2CrossProduct.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftAntiSemiJoinNotIn2CrossProduct
//
//	@doc:
//		Transform left anti semi join with NotIn semantics to cross product
//
//---------------------------------------------------------------------------
class CXformLeftAntiSemiJoinNotIn2CrossProduct
	: public CXformLeftAntiSemiJoin2CrossProduct
{
private:
	// private copy ctor
	CXformLeftAntiSemiJoinNotIn2CrossProduct(
		const CXformLeftAntiSemiJoinNotIn2CrossProduct &);

public:
	// ctor
	explicit CXformLeftAntiSemiJoinNotIn2CrossProduct(CMemoryPool *mp)
		:  // pattern
		  CXformLeftAntiSemiJoin2CrossProduct(GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoinNotIn(mp),
			  GPOS_NEW(mp) CExpression(
				  mp,
				  GPOS_NEW(mp) CPatternTree(
					  mp)),	 // left child is a tree since we may need to push predicates down
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
			  GPOS_NEW(mp) CExpression(
				  mp,
				  GPOS_NEW(mp) CPatternTree(
					  mp))	// predicate is a tree since we may need to do clean-up of scalar expression
			  ))
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftAntiSemiJoinNotIn2CrossProduct;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftAntiSemiJoinNotIn2CrossProduct";
	}

};	// class CXformLeftAntiSemiJoinNotIn2CrossProduct

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftAntiSemiJoinNotIn2CrossProduct_H

// EOF
