//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CJoinOrderMinCard.cpp
//
//	@doc:
//		Implementation of cardinality-based join order generation
//---------------------------------------------------------------------------

#include "gpopt/xforms/CJoinOrderMinCard.h"

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::CJoinOrderMinCard
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderMinCard::CJoinOrderMinCard(CMemoryPool *mp,
									 CExpressionArray *pdrgpexprComponents,
									 CExpressionArray *pdrgpexprConjuncts)
	: CJoinOrder(mp, pdrgpexprComponents, pdrgpexprConjuncts,
				 true /* m_include_loj_childs */),
	  m_pcompResult(NULL)
{
#ifdef GPOS_DEBUG
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		GPOS_ASSERT(NULL != m_rgpcomp[ul]->m_pexpr->Pstats() &&
					"stats were not derived on input component");
	}
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::~CJoinOrderMinCard
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderMinCard::~CJoinOrderMinCard()
{
	CRefCount::SafeRelease(m_pcompResult);
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderMinCard::PexprExpand()
{
	GPOS_ASSERT(NULL == m_pcompResult && "join order is already expanded");

	m_pcompResult = GPOS_NEW(m_mp) SComponent(m_mp, NULL /*pexpr*/);
	ULONG ulCoveredComps = 0;
	while (ulCoveredComps < m_ulComps)
	{
		CDouble dMinRows(0.0);
		SComponent *pcompBest =
			NULL;  // best component to be added to current result
		SComponent *pcompBestResult =
			NULL;  // result after adding best component

		for (ULONG ul = 0; ul < m_ulComps; ul++)
		{
			SComponent *pcompCurrent = m_rgpcomp[ul];
			if (pcompCurrent->m_fUsed)
			{
				// used components are already included in current result
				continue;
			}

			if (!IsValidJoinCombination(m_pcompResult, pcompCurrent))
			{
				continue;
			}

			// combine component with current result and derive stats
			CJoinOrder::SComponent *pcompTemp =
				PcompCombine(m_pcompResult, pcompCurrent);
			DeriveStats(pcompTemp->m_pexpr);
			CDouble rows = pcompTemp->m_pexpr->Pstats()->Rows();

			if (NULL == pcompBestResult || rows < dMinRows)
			{
				pcompBest = pcompCurrent;
				dMinRows = rows;
				pcompTemp->AddRef();
				CRefCount::SafeRelease(pcompBestResult);
				pcompBestResult = pcompTemp;
			}
			pcompTemp->Release();
		}

#ifndef GPOS_DEBUG
		if (pcompBest == NULL)
		{
			// ideally we should never have the best result as null
			GPOS_RAISE(CException::ExmaInvalid, CException::ExmiInvalid,
					   GPOS_WSZ_LIT("Unable to find best join component"));
		}
#endif

		GPOS_ASSERT(NULL != pcompBestResult);

		// mark best component as used.
		// we will never have p_compBest as NULL, as
		// we iterate over all the components to find the possible
		// join alternative, thus there will be atleast one component
		// marked as pcompBest
		pcompBest->m_fUsed = true;
		m_pcompResult->Release();
		m_pcompResult = pcompBestResult;

		// mark used edges to avoid including them multiple times
		MarkUsedEdges(m_pcompResult);
		ulCoveredComps++;
	}
	GPOS_ASSERT(NULL != m_pcompResult->m_pexpr);

	CExpression *pexprResult = m_pcompResult->m_pexpr;
	pexprResult->AddRef();

	return pexprResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderMinCard::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderMinCard::OsPrint(IOstream &os) const
{
	if (NULL != m_pcompResult->m_pexpr)
	{
		os << *m_pcompResult->m_pexpr;
	}

	return os;
}

// EOF
