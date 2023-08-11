//---------------------------------------------------------------------------
//	@filename:
//		CPartialPlan.cpp
//
//	@doc:
//		Implementation of partial plans created during optimization
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/engine/CPartialPlan.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::CPartialPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartialPlan::CPartialPlan(CGroupExpression* pgexpr, CReqdPropPlan* prpp, CCostContext* pccChild, ULONG child_index)
	: m_pgexpr(pgexpr), m_prpp(prpp), m_pccChild(pccChild), m_ulChildIndex(child_index)
{
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
}

//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::ExtractChildrenCostingInfo
//
//	@doc:
//		Extract costing info from children
//
//---------------------------------------------------------------------------
void CPartialPlan::ExtractChildrenCostingInfo(ICostModel* pcm, CExpressionHandle &exprhdl, ICostModel::SCostingInfo* pci)
{
	const ULONG arity = m_pgexpr->Arity();
	ULONG ulIndex = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CGroup* pgroupChild = (*m_pgexpr)[ul];
		if (pgroupChild->m_fScalar)
		{
			// skip scalar children
			continue;
		}
		CReqdPropPlan* prppChild = exprhdl.Prpp(ul);
		if (ul == m_ulChildIndex)
		{
			// we have reached a child with a known plan,
			// we have perfect costing information about this child
			// use provided child cost context to collect accurate costing info
			double dRowsChild = pgroupChild->m_listGExprs.front()->m_pop->estimated_cardinality;
			pci->SetChildRows(ulIndex, dRowsChild);
			// double dWidthChild = child_stats->Width(prppChild->m_pcrs).Get();
			// pci->SetChildWidth(ulIndex, dWidthChild);
			// pci->SetChildRebinds(ulIndex, child_stats->NumRebinds().Get());
			pci->SetChildCost(ulIndex, dRowsChild);
			// continue with next child
			ulIndex++;
			continue;
		}
		// otherwise, we do not know child plan yet,
		// we assume lower bounds on child row estimate and cost
		double dRowsChild = pgroupChild->m_listGExprs.front()->m_pop->estimated_cardinality;
		pci->SetChildRows(ulIndex, dRowsChild);
		// pci->SetChildRebinds(ulIndex, child_stats->NumRebinds().Get());
		// double dWidthChild = child_stats->Width(prppChild->m_pcrs).Get();
		// pci->SetChildWidth(ulIndex, dWidthChild);
		// use child group's cost lower bound as the child cost
		// double dCostChild = pgroupChild->CostLowerBound(prppChild).Get();
		pci->SetChildCost(ulIndex, dRowsChild);
		// advance to next child
		ulIndex++;
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
double CPartialPlan::CostCompute()
{
	CExpressionHandle exprhdl;
	exprhdl.Attach(m_pgexpr);
	// init required properties of expression
	exprhdl.DeriveProps(NULL);
	exprhdl.InitReqdProps(m_prpp);
	// create array of child derived properties
	duckdb::vector<CDrvdProp*> pdrgpdp;
	const ULONG arity = m_pgexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		// compute required columns of the n-th child
		exprhdl.ComputeChildReqdCols(ul, pdrgpdp);
	}
	Operator* pop = m_pgexpr->m_pop.get();
	// extract rows from stats
	double rows = pop->estimated_cardinality;
	// extract width from stats
	// double width = m_group_expression->Pgroup()->Pstats()->Width(mp, m_required_plan_property->m_pcrs).Get();
	// ci.SetWidth(width);
	// extract rebinds
	// double num_rebinds = m_group_expression->Pgroup()->Pstats()->NumRebinds().Get();
	// ci.SetRebinds(num_rebinds);
	// compute partial plan cost
	double cost = rows;
	if (0 < pop->children.size() && 1.0 < cost)
	{
		// cost model implementation adds an artificial const (1.0) to
		// sum of children cost,
		// we subtract this 1.0 here since we compute a lower bound
		// TODO:  05/07/2014: remove artificial const 1.0 in CostSum() function
		cost = cost - 1.0;
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
ULONG CPartialPlan::HashValue(const CPartialPlan *ppp)
{
	ULONG ulHash = ppp->m_pgexpr->HashValue();
	return CombineHashes(ulHash, CReqdPropPlan::UlHashForCostBounding(ppp->m_prpp));
}

//---------------------------------------------------------------------------
//	@function:
//		CPartialPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool CPartialPlan::Equals(const CPartialPlan *pppFst, const CPartialPlan *pppSnd)
{
	BOOL fEqual = false;
	if (NULL == pppFst->m_pccChild || NULL == pppSnd->m_pccChild)
	{
		fEqual = (NULL == pppFst->m_pccChild && NULL == pppSnd->m_pccChild);
	}
	else
	{
		// use pointers for fast comparison
		fEqual = (pppFst->m_pccChild == pppSnd->m_pccChild);
	}
	return fEqual && pppFst->m_ulChildIndex == pppSnd->m_ulChildIndex && pppFst->m_pgexpr == pppSnd->m_pgexpr && CReqdPropPlan::FEqualForCostBounding(pppFst->m_prpp, pppSnd->m_prpp);
}

ULONG CPartialPlan::HashValue() const
{
    ULONG ulHash = m_pgexpr->HashValue();
	return CombineHashes(ulHash, CReqdPropPlan::UlHashForCostBounding(m_prpp));
}

bool CPartialPlan::operator==(const CPartialPlan &pppSnd) const
{
	BOOL fEqual = false;
	if (nullptr == m_pccChild || nullptr == pppSnd.m_pccChild)
	{
		fEqual = (nullptr == m_pccChild && nullptr == pppSnd.m_pccChild);
	}
	else
	{
		// use pointers for fast comparison
		fEqual = (m_pccChild == pppSnd.m_pccChild);
	}
	return fEqual && m_ulChildIndex == pppSnd.m_ulChildIndex && m_pgexpr == pppSnd.m_pgexpr && CReqdPropPlan::FEqualForCostBounding(m_prpp, pppSnd.m_prpp);
}
}