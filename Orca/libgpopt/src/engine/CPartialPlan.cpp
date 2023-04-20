//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartialPlan.cpp
//
//	@doc:
//		Implementation of partial plans created during optimization
//---------------------------------------------------------------------------

#include "gpopt/engine/CPartialPlan.h"

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotion.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupExpression.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::CPartialPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartialPlan::CPartialPlan(CGroupExpression *pgexpr, CReqdPropPlan *prpp,
						   CCostContext *pccChild,
						   ULONG child_index)
	: m_pgexpr(pgexpr),	 // not owned
	  m_prpp(prpp),
	  m_pccChild(pccChild),	 // cost context of an already optimized child
	  m_ulChildIndex(child_index)
{
	GPOS_ASSERT(NULL != pgexpr);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT_IMP(NULL != pccChild, child_index < pgexpr->Arity());
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::~CPartialPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartialPlan::~CPartialPlan()
{
	m_prpp->Release();
	CRefCount::SafeRelease(m_pccChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::ExtractChildrenCostingInfo
//
//	@doc:
//		Extract costing info from children
//
//---------------------------------------------------------------------------
void
CPartialPlan::ExtractChildrenCostingInfo(CMemoryPool *mp, ICostModel *pcm,
										 CExpressionHandle &exprhdl,
										 ICostModel::SCostingInfo *pci)
{
	GPOS_ASSERT(m_pgexpr == exprhdl.Pgexpr());
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT_IMP(NULL != m_pccChild, m_ulChildIndex < exprhdl.Arity());

	const ULONG arity = m_pgexpr->Arity();
	ULONG ulIndex = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CGroup *pgroupChild = (*m_pgexpr)[ul];
		if (pgroupChild->FScalar())
		{
			// skip scalar children
			continue;
		}

		CReqdPropPlan *prppChild = exprhdl.Prpp(ul);
		IStatistics *child_stats = pgroupChild->Pstats();
		RaiseExceptionIfStatsNull(child_stats);

		if (ul == m_ulChildIndex)
		{
			// we have reached a child with a known plan,
			// we have perfect costing information about this child

			// use stats in provided child context
			child_stats = m_pccChild->Pstats();

			// use provided child cost context to collect accurate costing info
			DOUBLE dRowsChild = child_stats->Rows().Get();
			if (CDistributionSpec::EdptPartitioned ==
				m_pccChild->Pdpplan()->Pds()->Edpt())
			{
				// scale statistics row estimate by number of segments
				dRowsChild = pcm->DRowsPerHost(CDouble(dRowsChild)).Get();
			}

			pci->SetChildRows(ulIndex, dRowsChild);
			DOUBLE dWidthChild =
				child_stats->Width(mp, prppChild->PcrsRequired()).Get();
			pci->SetChildWidth(ulIndex, dWidthChild);
			pci->SetChildRebinds(ulIndex, child_stats->NumRebinds().Get());
			pci->SetChildCost(ulIndex, m_pccChild->Cost().Get());

			// continue with next child
			ulIndex++;
			continue;
		}

		// otherwise, we do not know child plan yet,
		// we assume lower bounds on child row estimate and cost
		DOUBLE dRowsChild = child_stats->Rows().Get();
		dRowsChild = pcm->DRowsPerHost(CDouble(dRowsChild)).Get();
		pci->SetChildRows(ulIndex, dRowsChild);

		pci->SetChildRebinds(ulIndex, child_stats->NumRebinds().Get());

		DOUBLE dWidthChild =
			child_stats->Width(mp, prppChild->PcrsRequired()).Get();
		pci->SetChildWidth(ulIndex, dWidthChild);

		// use child group's cost lower bound as the child cost
		DOUBLE dCostChild = pgroupChild->CostLowerBound(mp, prppChild).Get();
		pci->SetChildCost(ulIndex, dCostChild);

		// advance to next child
		ulIndex++;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::RaiseExceptionIfStatsNull
//
//	@doc:
//		Raise exception if the stats object is NULL
//
//---------------------------------------------------------------------------
void
CPartialPlan::RaiseExceptionIfStatsNull(IStatistics *stats)
{
	if (NULL == stats)
	{
		GPOS_RAISE(
			gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound,
			GPOS_WSZ_LIT(
				"Could not compute cost of partial plan since statistics for the group not derived"));
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::CostCompute
//
//	@doc:
//		Compute partial plan cost
//
//---------------------------------------------------------------------------
CCost
CPartialPlan::CostCompute(CMemoryPool *mp)
{
	CExpressionHandle exprhdl(mp);
	exprhdl.Attach(m_pgexpr);

	// init required properties of expression
	exprhdl.DeriveProps(NULL /*pdpdrvdCtxt*/);
	exprhdl.InitReqdProps(m_prpp);

	// create array of child derived properties
	CDrvdPropArray *pdrgpdp = GPOS_NEW(mp) CDrvdPropArray(mp);
	const ULONG arity = m_pgexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		// compute required columns of the n-th child
		exprhdl.ComputeChildReqdCols(ul, pdrgpdp);
	}
	pdrgpdp->Release();

	IStatistics *stats = m_pgexpr->Pgroup()->Pstats();
	RaiseExceptionIfStatsNull(stats);

	stats->AddRef();
	ICostModel::SCostingInfo ci(mp, exprhdl.UlNonScalarChildren(),
								GPOS_NEW(mp) ICostModel::CCostingStats(stats));

	ICostModel *pcm = COptCtxt::PoctxtFromTLS()->GetCostModel();
	ExtractChildrenCostingInfo(mp, pcm, exprhdl, &ci);

	CDistributionSpec::EDistributionPartitioningType edpt =
		CDistributionSpec::EdptSentinel;
	if (NULL != m_prpp->Ped())
	{
		edpt = m_prpp->Ped()->PdsRequired()->Edpt();
	}

	COperator *pop = m_pgexpr->Pop();
	BOOL fDataPartitioningMotion =
		CUtils::FPhysicalMotion(pop) &&
		CDistributionSpec::EdptPartitioned ==
			CPhysicalMotion::PopConvert(pop)->Pds()->Edpt();

	// extract rows from stats
	DOUBLE rows = m_pgexpr->Pgroup()->Pstats()->Rows().Get();
	if (fDataPartitioningMotion ||	// root operator is known to distribute data across segments
		NULL ==
			m_prpp
				->Ped() ||	// required distribution not known yet, we assume data partitioning since we need a lower-bound on number of rows
		CDistributionSpec::EdptPartitioned ==
			edpt ||	 // required distribution is known to be partitioned, we assume data partitioning since we need a lower-bound on number of rows
		CDistributionSpec::EdptUnknown ==
			edpt  // required distribution is not known to be partitioned (e.g., ANY distribution), we assume data partitioning since we need a lower-bound on number of rows
	)
	{
		// use rows per host as a cardinality lower bound
		rows = pcm->DRowsPerHost(CDouble(rows)).Get();
	}
	ci.SetRows(rows);

	// extract width from stats
	DOUBLE width =
		m_pgexpr->Pgroup()->Pstats()->Width(mp, m_prpp->PcrsRequired()).Get();
	ci.SetWidth(width);

	// extract rebinds
	DOUBLE num_rebinds = m_pgexpr->Pgroup()->Pstats()->NumRebinds().Get();
	ci.SetRebinds(num_rebinds);

	// compute partial plan cost
	CCost cost = pcm->Cost(exprhdl, &ci);

	if (0 < ci.ChildCount() && 1.0 < cost.Get())
	{
		// cost model implementation adds an artificial const (1.0) to
		// sum of children cost,
		// we subtract this 1.0 here since we compute a lower bound

		// TODO:  05/07/2014: remove artificial const 1.0 in CostSum() function
		cost = CCost(cost.Get() - 1.0);
	}

	return cost;
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPartialPlan::HashValue(const CPartialPlan *ppp)
{
	GPOS_ASSERT(NULL != ppp);

	ULONG ulHash = ppp->Pgexpr()->HashValue();
	return CombineHashes(ulHash,
						 CReqdPropPlan::UlHashForCostBounding(ppp->Prpp()));
}


//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CPartialPlan::Equals(const CPartialPlan *pppFst, const CPartialPlan *pppSnd)
{
	GPOS_ASSERT(NULL != pppFst);
	GPOS_ASSERT(NULL != pppSnd);

	BOOL fEqual = false;
	if (NULL == pppFst->PccChild() || NULL == pppSnd->PccChild())
	{
		fEqual = (NULL == pppFst->PccChild() && NULL == pppSnd->PccChild());
	}
	else
	{
		// use pointers for fast comparison
		fEqual = (pppFst->PccChild() == pppSnd->PccChild());
	}

	return fEqual && pppFst->UlChildIndex() == pppSnd->UlChildIndex() &&
		   pppFst->Pgexpr() ==
			   pppSnd->Pgexpr() &&	// use pointers for fast comparison
		   CReqdPropPlan::FEqualForCostBounding(pppFst->Prpp(), pppSnd->Prpp());
}


// EOF
