//---------------------------------------------------------------------------
//	@filename:
//		COptimizationContext.cpp
//
//	@doc:
//		Implementation of optimization context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CEnfdOrder.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"

namespace gpopt
{
// invalid optimization context
const COptimizationContext COptimizationContext::m_ocInvalid;

// invalid optimization context pointer
const OPTCTXT_PTR COptimizationContext::m_pocInvalid = NULL;

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::~COptimizationContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COptimizationContext::~COptimizationContext()
{
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::PgexprBest
//
//	@doc:
//		Best group expression accessor
//
//---------------------------------------------------------------------------
CGroupExpression* COptimizationContext::PgexprBest() const
{
	if (nullptr == m_pccBest)
	{
		return nullptr;
	}
	return m_pccBest->m_group_expression;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::SetBest
//
//	@doc:
//		 Set best cost context
//
//---------------------------------------------------------------------------
void COptimizationContext::SetBest(CCostContext* pcc)
{
	m_pccBest = pcc;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::Matches
//
//	@doc:
//		Match against another context
//
//---------------------------------------------------------------------------
bool COptimizationContext::Matches(const COptimizationContext* poc) const
{
	if (m_pgroup != poc->m_pgroup || m_ulSearchStageIndex != poc->UlSearchStageIndex())
	{
		return false;
	}
	CReqdPropPlan* prppFst = this->m_prpp;
	CReqdPropPlan* prppSnd = poc->m_prpp;
	// make sure we are not comparing to invalid context
	if (NULL == prppFst || NULL == prppSnd)
	{
		return NULL == prppFst && NULL == prppSnd;
	}
	return prppFst->Equals(prppSnd);
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FEqualForStats
//
//	@doc:
//		Equality function used for computing stats during costing
//
//---------------------------------------------------------------------------
bool COptimizationContext::FEqualForStats(const COptimizationContext *pocLeft, const COptimizationContext *pocRight)
{
	return CUtils::Equals(pocLeft->m_prprel->PcrsStat(), pocRight->m_prprel->PcrsStat());
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimize
//
//	@doc:
//		Return true if given group expression should be optimized under
//		given context
//
//---------------------------------------------------------------------------
bool COptimizationContext::FOptimize(CGroupExpression* pgexprParent, CGroupExpression* pgexprChild, COptimizationContext* pocChild, ULONG ulSearchStages)
{
	if (PhysicalOperatorType::ORDER_BY == pgexprChild->m_pop->physical_type)
	{
		return FOptimizeSort(pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}
	if (PhysicalOperatorType::NESTED_LOOP_JOIN == pgexprChild->m_pop->physical_type)
	{
		return FOptimizeNLJoin(pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FEqualIds
//
//	@doc:
//		Compare array of optimization contexts based on context ids
//
//---------------------------------------------------------------------------
bool COptimizationContext::FEqualContextIds(duckdb::vector<COptimizationContext*> pdrgpocFst, duckdb::vector<COptimizationContext*> pdrgpocSnd)
{
	if (0 == pdrgpocFst.size() || 0 == pdrgpocSnd.size())
	{
		return (0 == pdrgpocFst.size() && 0 == pdrgpocSnd.size());
	}
	const ULONG ulCtxts = pdrgpocFst.size();
	if (ulCtxts != pdrgpocSnd.size())
	{
		return false;
	}
	bool fEqual = true;
	for (ULONG ul = 0; fEqual && ul < ulCtxts; ul++)
	{
		fEqual = pdrgpocFst[ul]->m_id == pdrgpocSnd[ul]->m_id;
	}
	return fEqual;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeSort
//
//	@doc:
//		Check if a Sort node should be optimized for the given context
//
//---------------------------------------------------------------------------
bool COptimizationContext::FOptimizeSort(CGroupExpression* pgexprParent, CGroupExpression* pgexprSort, COptimizationContext* poc, ULONG ulSearchStages)
{
	return poc->m_prpp->m_peo->FCompatible(((PhysicalOrder*)pgexprSort->m_pop.get())->Pos());
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeAgg
//
//	@doc:
//		Check if Agg node should be optimized for the given context
//
//---------------------------------------------------------------------------
bool COptimizationContext::FOptimizeAgg(CGroupExpression* pgexprParent, CGroupExpression* pgexprAgg, COptimizationContext* poc, ULONG ulSearchStages)
{
	// otherwise, we need to avoid optimizing node unless it is a multi-stage agg
	COptimizationContext* pocFound = pgexprAgg->m_pgroup->PocLookupBest(ulSearchStages, poc->m_prpp);
	// if (NULL != pocFound && pocFound->FHasMultiStageAggPlan())
	// {
	//  	// context already has a multi-stage agg plan, optimize child only if it is also a multi-stage agg
	// 	    return CPhysicalAgg::PopConvert(pgexprAgg->Pop())->FMultiStage();
	// }
	// child context has no plan yet, return true
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::FOptimizeNLJoin
//
//	@doc:
//		Check if NL join node should be optimized for the given context
//
//---------------------------------------------------------------------------
bool COptimizationContext::FOptimizeNLJoin(CGroupExpression* pgexprParent, CGroupExpression* pgexprJoin, COptimizationContext* poc, ULONG ulSearchStages)
{
	// For correlated join, the requested columns must be covered by outer child
	// columns and columns to be generated from inner child
	duckdb::vector<ColumnBinding> pcrs;
	duckdb::vector<ColumnBinding> pcrsOuterChild = CDrvdPropRelational::GetRelationalProperties((*pgexprJoin)[0]->m_pdp)->GetOutputColumns();
	pcrs.insert(pcrsOuterChild.begin(), pcrsOuterChild.end(), pcrs.end());
	bool fIncluded = CUtils::ContainsAll(pcrs, poc->m_prpp->m_pcrs);
	return fIncluded;
}
}