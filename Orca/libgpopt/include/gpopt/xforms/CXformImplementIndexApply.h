//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Software, Inc.
//
//	Template Class for Inner / Left Outer Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementIndexApply_H
#define GPOPT_CXformImplementIndexApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

class CXformImplementIndexApply : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementIndexApply(const CXformImplementIndexApply &);

public:
	// ctor
	explicit CXformImplementIndexApply(CMemoryPool *mp)
		:  // pattern
		  CXformImplementation(GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalIndexApply(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // outer child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // inner child
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
			  ))
	{
	}

	// dtor
	virtual ~CXformImplementIndexApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementIndexApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementIndexApply";
	}

	virtual EXformPromise
	Exfp(CExpressionHandle &exprhdl) const
	{
		if (exprhdl.DeriveHasSubquery(2))
		{
			return ExfpNone;
		}
		return ExfpHigh;
	}

	// actual transform
	virtual void
	Transform(CXformContext *pxfctxt, CXformResult *pxfres,
			  CExpression *pexpr) const
	{
		GPOS_ASSERT(NULL != pxfctxt);
		GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
		GPOS_ASSERT(FCheckPattern(pexpr));

		CMemoryPool *mp = pxfctxt->Pmp();

		// extract components
		CExpression *pexprOuter = (*pexpr)[0];
		CExpression *pexprInner = (*pexpr)[1];
		CExpression *pexprScalar = (*pexpr)[2];
		CColRefArray *colref_array =
			CLogicalIndexApply::PopConvert(pexpr->Pop())->PdrgPcrOuterRefs();
		colref_array->AddRef();

		// addref all components
		pexprOuter->AddRef();
		pexprInner->AddRef();
		pexprScalar->AddRef();

		// assemble physical operator
		CPhysicalNLJoin *pop = NULL;

		if (CLogicalIndexApply::PopConvert(pexpr->Pop())->FouterJoin())
			pop = GPOS_NEW(mp) CPhysicalLeftOuterIndexNLJoin(mp, colref_array);
		else
			pop = GPOS_NEW(mp) CPhysicalInnerIndexNLJoin(mp, colref_array);

		CExpression *pexprResult = GPOS_NEW(mp)
			CExpression(mp, pop, pexprOuter, pexprInner, pexprScalar);

		// add alternative to results
		pxfres->Add(pexprResult);
	}

};	// class CXformImplementIndexApply

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementIndexApply_H

// EOF
