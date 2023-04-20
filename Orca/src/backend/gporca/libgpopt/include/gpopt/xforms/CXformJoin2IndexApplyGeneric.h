//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (c) 2020 VMware and affiliates, Inc.
//
// Transform a join into an index apply. Allow a variety of nodes on
// the innser side, including a mandatory get, plus optional select,
// project and aggregate nodes.
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoin2IndexApplyGeneric_H
#define GPOPT_CXformJoin2IndexApplyGeneric_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternNode.h"
#include "gpopt/xforms/CXformJoin2IndexApply.h"

namespace gpopt
{
using namespace gpos;

class CXformJoin2IndexApplyGeneric : public CXformJoin2IndexApply
{
private:
	// this decides which types of plans are produced, index gets or bitmap gets
	BOOL m_generateBitmapPlans;

	// Can we transform left outer join to left outer index apply?
	BOOL FCanLeftOuterIndexApply(CMemoryPool *mp, CExpression *pexprInner,
								 CExpression *pexprScalar,
								 CTableDescriptor *ptabDesc,
								 const CColRefSet *pcrsDist) const;

public:
	CXformJoin2IndexApplyGeneric(const CXformJoin2IndexApplyGeneric &) = delete;

	// ctor
	explicit CXformJoin2IndexApplyGeneric(CMemoryPool *mp,
										  BOOL generateBitmapPlans)
		:  // pattern
		  CXformJoin2IndexApply(GPOS_NEW(mp) CExpression(
			  mp,
			  GPOS_NEW(mp)
				  CPatternNode(mp, CPatternNode::EmtMatchInnerOrLeftOuterJoin),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // outer child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp)),  // inner child
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(
											   mp))	 // predicate tree operator,
			  )),
		  m_generateBitmapPlans(generateBitmapPlans)
	{
	}

	// dtor
	~CXformJoin2IndexApplyGeneric() override = default;

	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

	// Return true if xform should be applied only once.
	// For now return true. We may need to revisit this if we find that
	// there are multiple bindings and we miss interesting bindings because
	// we extract only one of them.
	BOOL
	IsApplyOnce() override
	{
		return true;
	}

};	// class CXformJoin2IndexApplyGeneric

}  // namespace gpopt

#endif	// !GPOPT_CXformJoin2IndexApplyGeneric_H

// EOF
