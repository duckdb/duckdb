//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementUnionAll.cpp
//
//	@doc:
//		Implementation of union all operator
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementUnionAll.h"

#include "gpos/base.h"

#include "gpopt/exception.h"
#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpopt/operators/CPhysicalUnionAllFactory.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementUnionAll::CXformImplementUnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementUnionAll::CXformImplementUnionAll(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalUnionAll(mp),
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementUnionAll::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementUnionAll::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CLogicalUnionAll *popUnionAll = CLogicalUnionAll::PopConvert(pexpr->Pop());
	CPhysicalUnionAllFactory factory(popUnionAll);

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexpr->Arity();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	CPhysicalUnionAll *popPhysicalSerialUnionAll =
		factory.PopPhysicalUnionAll(mp, false);

	// assemble serial union physical operator
	CExpression *pexprSerialUnionAll =
		GPOS_NEW(mp) CExpression(mp, popPhysicalSerialUnionAll, pdrgpexpr);

	// add serial union alternative to results
	pxfres->Add(pexprSerialUnionAll);

	// parallel union alternative to the result if the GUC is on
	BOOL fParallel = GPOS_FTRACE(EopttraceEnableParallelAppend);

	if (fParallel)
	{
		CPhysicalUnionAll *popPhysicalParallelUnionAll =
			factory.PopPhysicalUnionAll(mp, true);

		pdrgpexpr->AddRef();

		// assemble physical parallel operator
		CExpression *pexprParallelUnionAll = GPOS_NEW(mp)
			CExpression(mp, popPhysicalParallelUnionAll, pdrgpexpr);

		// add parallel union alternative to results
		pxfres->Add(pexprParallelUnionAll);
	}
}

// EOF
