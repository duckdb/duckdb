//---------------------------------------------------------------------------
//	@filename:
//		CCostContext.cpp
//
//	@doc:
//		Implementation of cost context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtRelational.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include <cstdlib>

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::CCostContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCostContext::CCostContext(COptimizationContext* poc, ULONG ulOptReq, CGroupExpression* pgexpr)
	:m_cost(GPOPT_INVALID_COST), m_estate(estUncosted), m_group_expression(pgexpr), m_pgexprForStats(nullptr), m_pdpplan(nullptr), m_ulOptReq(ulOptReq), m_fPruned(false), m_poc(poc)
{
	if(m_group_expression != nullptr)
	{
		CGroupExpression* pgexprForStats = m_group_expression->m_pgroup->PgexprBestPromise(m_group_expression);
		if (nullptr != pgexprForStats)
		{
			m_pgexprForStats = pgexprForStats;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::~CCostContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCostContext::~CCostContext()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FNeedsNewStats
//
//	@doc:
//		Check if we need to derive new stats for this context,
//		by default a cost context inherits stats from the owner group,
//		the only current exception is when part of the plan below cost
//		context is affected by partition elimination done by partition
//		selection in some other part of the plan
//
//---------------------------------------------------------------------------
bool CCostContext::FNeedsNewStats() const
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::DerivePlanProps
//
//	@doc:
//		Derive properties of the plan carried by cost context
//
//---------------------------------------------------------------------------
void CCostContext::DerivePlanProps()
{
	if (nullptr == m_pdpplan)
	{
		// derive properties of the plan carried by cost context
		CExpressionHandle exprhdl;
		exprhdl.Attach(this);
		exprhdl.DerivePlanPropsForCostContext();
		CDrvdPropPlan* pdpplan = CDrvdPropPlan::Pdpplan(exprhdl.Pdp());
		m_pdpplan = pdpplan;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::operator ==
//
//	@doc:
//		Comparison operator
//
//---------------------------------------------------------------------------
bool CCostContext::operator==(const CCostContext &cc) const
{
	return Equals(cc, *this);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostContext::IsValid
//
//	@doc:
//		Check validity by comparing derived and required properties
//
//---------------------------------------------------------------------------
bool CCostContext::IsValid()
{
	// obtain relational properties from group
	CDrvdPropRelational* pdprel = CDrvdPropRelational::GetRelationalProperties(m_group_expression->m_pgroup->m_pdp);
	// derive plan properties
	DerivePlanProps();
	// checking for required properties satisfaction
	bool fValid = m_poc->m_prpp->FSatisfied(pdprel, m_pdpplan);
	return fValid;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FBreakCostTiesForJoinPlan
//
//	@doc:
//		For two cost contexts with join plans of the same cost, break the
//		tie in cost values based on join depth,
//		if tie-resolution succeeded, store a pointer to preferred cost
//		context in output argument
//
//---------------------------------------------------------------------------
void CCostContext::BreakCostTiesForJoinPlans(CCostContext* pccFst, CCostContext* pccSnd, CCostContext** ppccPrefered, bool* pfTiesResolved)
{
	// for two join plans with the same estimated rows in both children,
	// prefer the plan that has smaller tree depth on the inner side,
	// this is because a smaller tree depth means that row estimation
	// errors are not grossly amplified,
	// since we build a hash table/broadcast the inner side, we need
	// to have more reliable statistics on this side
	*pfTiesResolved = false;
	*ppccPrefered = nullptr;
	double dRowsOuterFst = pccFst->m_pdrgpoc[0]->m_pccBest->m_cost;
	double dRowsInnerFst = pccFst->m_pdrgpoc[1]->m_pccBest->m_cost;
	if (dRowsOuterFst != dRowsInnerFst)
	{
		// two children of first plan have different row estimates
		return;
	}
	double dRowsOuterSnd = pccSnd->m_pdrgpoc[0]->m_pccBest->m_cost;
	double dRowsInnerSnd = pccSnd->m_pdrgpoc[1]->m_pccBest->m_cost;
	if (dRowsOuterSnd != dRowsInnerSnd)
	{
		// two children of second plan have different row estimates
		return;
	}
	if (dRowsInnerFst != dRowsInnerSnd)
	{
		// children of first plan have different row estimates compared to second plan
		return;
	}
	// both plans have equal estimated rows for both children, break tie based on join depth
	*pfTiesResolved = true;
	ULONG ulOuterJoinDepthFst = CDrvdPropRelational::GetRelationalProperties((*pccFst->m_group_expression)[0]->m_pdp)->GetJoinDepth();
	ULONG ulInnerJoinDepthFst = CDrvdPropRelational::GetRelationalProperties((*pccFst->m_group_expression)[1]->m_pdp)->GetJoinDepth();
	if (ulInnerJoinDepthFst < ulOuterJoinDepthFst)
	{
		*ppccPrefered = pccFst;
	}
	else
	{
		*ppccPrefered = pccSnd;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FBetterThan
//
//	@doc:
//		Is current context better than the given equivalent context
//		based on cost?
//
//---------------------------------------------------------------------------
bool CCostContext::FBetterThan(CCostContext* pcc) const
{
	double dCostDiff = (m_cost - pcc->m_cost);
	if (dCostDiff < 0.0)
	{
		// if current context has a strictly smaller cost, then it is preferred
		return true;
	}

	if (dCostDiff > 0.0)
	{
		// if current context has a strictly larger cost, then it is not preferred
		return false;
	}
	/* I comment here */
	/*
	// otherwise, we need to break tie in cost values
	// RULE 1: break ties in cost of join plans,
	// if both plans have the same estimated rows for both children, prefer
	// the plan with deeper outer child
	if (CUtils::FPhysicalJoin(Pgexpr()->Pop()) && CUtils::FPhysicalJoin(pcc->Pgexpr()->Pop()))
	{
		CONST_COSTCTXT_PTR pccPrefered = nullptr;
		bool fSuccess = false;
		BreakCostTiesForJoinPlans(this, pcc, &pccPrefered, &fSuccess);
		if (fSuccess)
		{
			return (this == pccPrefered);
		}
	}
	*/
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::CostCompute
//
//	@doc:
//		Compute cost of current context,
//
//		the function extracts cardinality and row width of owner operator
//		and child operators, and then adjusts row estimate obtained from
//		statistics based on data distribution obtained from plan properties,
//
//		statistics row estimate is computed on logical expressions by
//		estimating the size of the whole relation regardless data
//		distribution, on the other hand, optimizer's cost model computes
//		the cost of a plan instance on some segment,
//
//		when a plan produces tuples distributed to multiple segments, we
//		need to divide statistics row estimate by the number segments to
//		provide a per-segment row estimate for cost computation,
//
//		Note that this scaling of row estimate cannot happen during
//		statistics derivation since plans are not created yet at this point
//
// 		this function also extracts number of rebinds of owner operator child
//		operators, if statistics are computed using predicates with external
//		parameters (outer references), number of rebinds is the total number
//		of external parameters' values
//
//---------------------------------------------------------------------------
double CCostContext::CostCompute(duckdb::vector<double> pdrgpcostChildren)
{
	/* I comment here */
	/*
	// derive context stats
	DeriveStats();
	ULONG arity = 0;
	if (nullptr != m_pdrgpoc)
	{
		arity = Pdrgpoc()->Size();
	}
	m_pstats->AddRef();
	ICostModel::SCostingInfo ci(arity, new ICostModel::CCostingStats(m_pstats));
	ICostModel* pcm = COptCtxt::PoctxtFromTLS()->m_cost_model;
	CExpressionHandle exprhdl();
	exprhdl.Attach(this);
	// extract local costing info
	DOUBLE rows = m_pstats->Rows().Get();
	ci.SetRows(rows);
	DOUBLE width = m_pstats->Width(m_poc->m_required_plan_property->m_pcrs).Get();
	ci.SetWidth(width);
	DOUBLE num_rebinds = m_pstats->NumRebinds().Get();
	ci.SetRebinds(num_rebinds);
	GPOS_ASSERT_IMP(!exprhdl.HasOuterRefs(), GPOPT_DEFAULT_REBINDS == (ULONG)(num_rebinds) && "invalid number of rebinds when there are no outer references");
	// extract children costing info
	for (ULONG ul = 0; ul < arity; ul++)
	{
		COptimizationContext* pocChild = (*m_pdrgpoc)[ul];
		CCostContext* pccChild = pocChild->PccBest();
		IStatistics* child_stats = pccChild->Pstats();
		child_stats->AddRef();
		ci.SetChildStats(ul, new ICostModel::CCostingStats(child_stats));
		DOUBLE dRowsChild = child_stats->Rows().Get();
		ci.SetChildRows(ul, dRowsChild);
		DOUBLE dWidthChild = child_stats->Width(pocChild->m_required_plan_property->m_pcrs).Get();
		ci.SetChildWidth(ul, dWidthChild);
		DOUBLE dRebindsChild = child_stats->NumRebinds().Get();
		ci.SetChildRebinds(ul, dRebindsChild);
		GPOS_ASSERT_IMP(!exprhdl.HasOuterRefs(ul), GPOPT_DEFAULT_REBINDS == (ULONG)(dRebindsChild) && "invalid number of rebinds when there are no outer references");
		DOUBLE dCostChild = (*pdrgpcostChildren)[ul]->Get();
		ci.SetChildCost(ul, dCostChild);
	}
	// compute cost using the underlying cost model
	return pcm->Cost(exprhdl, &ci);
	*/
	return (double)(rand() % 1000) / 1000.0; 
}