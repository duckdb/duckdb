//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformPushGbBelowSetOp.h
//
//	@doc:
//		Push grouping below set operation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushGbBelowSetOp_H
#define GPOPT_CXformPushGbBelowSetOp_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushGbBelowSetOp
//
//	@doc:
//		Push grouping below set operation
//
//---------------------------------------------------------------------------
template <class TSetOp>
class CXformPushGbBelowSetOp : public CXformExploration
{
private:
	// private copy ctor
	CXformPushGbBelowSetOp(const CXformPushGbBelowSetOp &);

public:
	// ctor
	explicit CXformPushGbBelowSetOp(CMemoryPool *mp)
		: CXformExploration(
			  // pattern
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
				  GPOS_NEW(mp) CExpression	// left child is a set operation
				  (mp, GPOS_NEW(mp) TSetOp(mp),
				   GPOS_NEW(mp)
					   CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))),
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp)
							  CPatternTree(mp))	 // project list of group-by
				  ))
	{
	}

	// dtor
	virtual ~CXformPushGbBelowSetOp()
	{
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &exprhdl) const
	{
		CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
		if (popGbAgg->FGlobal())
		{
			return ExfpHigh;
		}

		return ExfpNone;
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
		COptimizerConfig *optconfig =
			COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();

		CExpression *pexprSetOp = (*pexpr)[0];
		if (pexprSetOp->Arity() >
			optconfig->GetHint()->UlPushGroupByBelowSetopThreshold())
		{
			// bail-out if set op has many children
			return;
		}
		CExpression *pexprPrjList = (*pexpr)[1];
		if (0 < pexprPrjList->Arity())
		{
			// bail-out if group-by has any aggregate functions
			return;
		}

		CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
		CLogicalSetOp *popSetOp = CLogicalSetOp::PopConvert(pexprSetOp->Pop());

		CColRefArray *pdrgpcrGb = popGbAgg->Pdrgpcr();
		CColRefArray *pdrgpcrOutput = popSetOp->PdrgpcrOutput();
		CColRefSet *pcrsOutput = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrOutput);
		CColRef2dArray *pdrgpdrgpcrInput = popSetOp->PdrgpdrgpcrInput();
		CExpressionArray *pdrgpexprNewChildren =
			GPOS_NEW(mp) CExpressionArray(mp);
		CColRef2dArray *pdrgpdrgpcrNewInput = GPOS_NEW(mp) CColRef2dArray(mp);
		const ULONG arity = pexprSetOp->Arity();

		BOOL fNewChild = false;

		for (ULONG ulChild = 0; ulChild < arity; ulChild++)
		{
			CExpression *pexprChild = (*pexprSetOp)[ulChild];
			CColRefArray *pdrgpcrChild = (*pdrgpdrgpcrInput)[ulChild];
			CColRefSet *pcrsChild = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrChild);

			CColRefArray *pdrgpcrChildGb = NULL;
			if (!pcrsChild->Equals(pcrsOutput))
			{
				// use column mapping in SetOp to set child grouping colums
				UlongToColRefMap *colref_mapping =
					CUtils::PhmulcrMapping(mp, pdrgpcrOutput, pdrgpcrChild);
				pdrgpcrChildGb = CUtils::PdrgpcrRemap(
					mp, pdrgpcrGb, colref_mapping, true /*must_exist*/);
				colref_mapping->Release();
			}
			else
			{
				// use grouping columns directly as child grouping colums
				pdrgpcrGb->AddRef();
				pdrgpcrChildGb = pdrgpcrGb;
			}

			pexprChild->AddRef();
			pcrsChild->Release();

			// if child of setop is already an Agg with the same grouping columns
			// that we want to use, there is no need to add another agg on top of it
			COperator *popChild = pexprChild->Pop();
			if (COperator::EopLogicalGbAgg == popChild->Eopid())
			{
				CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(popChild);
				if (CColRef::Equals(popGbAgg->Pdrgpcr(), pdrgpcrChildGb))
				{
					pdrgpexprNewChildren->Append(pexprChild);
					pdrgpdrgpcrNewInput->Append(pdrgpcrChildGb);

					continue;
				}
			}

			fNewChild = true;
			pexprPrjList->AddRef();
			CExpression *pexprChildGb = CUtils::PexprLogicalGbAggGlobal(
				mp, pdrgpcrChildGb, pexprChild, pexprPrjList);
			pdrgpexprNewChildren->Append(pexprChildGb);

			pdrgpcrChildGb->AddRef();
			pdrgpdrgpcrNewInput->Append(pdrgpcrChildGb);
		}

		pcrsOutput->Release();

		if (!fNewChild)
		{
			// all children of the union were already Aggs with the same grouping
			// columns that we would have created. No new alternative expressions
			pdrgpdrgpcrNewInput->Release();
			pdrgpexprNewChildren->Release();

			return;
		}

		pdrgpcrGb->AddRef();
		TSetOp *popSetOpNew =
			GPOS_NEW(mp) TSetOp(mp, pdrgpcrGb, pdrgpdrgpcrNewInput);
		CExpression *pexprNewSetOp =
			GPOS_NEW(mp) CExpression(mp, popSetOpNew, pdrgpexprNewChildren);

		popGbAgg->AddRef();
		pexprPrjList->AddRef();
		CExpression *pexprResult =
			GPOS_NEW(mp) CExpression(mp, popGbAgg, pexprNewSetOp, pexprPrjList);

		pxfres->Add(pexprResult);
	}

};	// class CXformPushGbBelowSetOp

}  // namespace gpopt

#endif	// !GPOPT_CXformPushGbBelowSetOp_H

// EOF
