//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCorrelatedApply.h
//
//	@doc:
//		Base class for implementing correlated NLJ
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementCorrelatedApply_H
#define GPOPT_CXformImplementCorrelatedApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"
#include "gpopt/xforms/CXformUtils.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementCorrelatedApply
//
//	@doc:
//		Implement correlated Apply
//
//---------------------------------------------------------------------------
template <class TLogicalApply, class TPhysicalJoin>
class CXformImplementCorrelatedApply : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementCorrelatedApply(const CXformImplementCorrelatedApply &);


public:
	// ctor
	explicit CXformImplementCorrelatedApply(CMemoryPool *mp)
		:  // pattern
		  CXformImplementation(GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) TLogicalApply(mp),
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
			  ))
	{
	}

	// dtor
	virtual ~CXformImplementCorrelatedApply()
	{
	}

	virtual EXformPromise
	Exfp(CExpressionHandle &exprhdl) const
	{
		if (exprhdl.DeriveHasSubquery(2))
		{
			return CXform::ExfpNone;
		}
		return CXform::ExfpHigh;
	}

	// actual transform
	void
	Transform(CXformContext *pxfctxt, CXformResult *pxfres,
			  CExpression *pexpr) const
	{
		GPOS_ASSERT(NULL != pxfctxt);
		GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
		GPOS_ASSERT(FCheckPattern(pexpr));

		CMemoryPool *mp = pxfctxt->Pmp();

		// extract components
		CExpression *pexprLeft = (*pexpr)[0];
		CExpression *pexprRight = (*pexpr)[1];
		CExpression *pexprScalar = (*pexpr)[2];
		TLogicalApply *popApply = TLogicalApply::PopConvert(pexpr->Pop());
		CColRefArray *colref_array = popApply->PdrgPcrInner();

		colref_array->AddRef();

		// addref all children
		pexprLeft->AddRef();
		pexprRight->AddRef();
		pexprScalar->AddRef();

		// assemble physical operator
		CExpression *pexprPhysicalApply = GPOS_NEW(mp) CExpression(
			mp,
			GPOS_NEW(mp)
				TPhysicalJoin(mp, colref_array, popApply->EopidOriginSubq()),
			pexprLeft, pexprRight, pexprScalar);

		// add alternative to results
		pxfres->Add(pexprPhysicalApply);
	}

};	// class CXformImplementCorrelatedApply

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementCorrelatedApply_H

// EOF
