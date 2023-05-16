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
#include "duckdb/optimizer/cascade/operators/CPhysicalAgg.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotion.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalNLJoin.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalSort.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalCTEProducer.h"

using namespace gpopt;

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
	CRefCount::SafeRelease(m_prpp);
	CRefCount::SafeRelease(m_prprel);
	CRefCount::SafeRelease(m_pdrgpstatCtxt);
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
	if (NULL == m_pccBest)
	{
		return NULL;
	}
	return m_pccBest->Pgexpr();
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::SetBest
//
//	@doc:
//		 Set best cost context
//
//---------------------------------------------------------------------------
void COptimizationContext::SetBest(CCostContext *pcc)
{
	GPOS_ASSERT(NULL != pcc);
	m_pccBest = pcc;
	COperator *pop = pcc->Pgexpr()->Pop();
	if (CUtils::FPhysicalAgg(pop) && CPhysicalAgg::PopConvert(pop)->FMultiStage())
	{
		m_fHasMultiStageAggPlan = true;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::Matches
//
//	@doc:
//		Match against another context
//
//---------------------------------------------------------------------------
BOOL COptimizationContext::Matches(const COptimizationContext *poc) const
{
	GPOS_ASSERT(NULL != poc);
	if (m_pgroup != poc->Pgroup() || m_ulSearchStageIndex != poc->UlSearchStageIndex())
	{
		return false;
	}
	CReqdPropPlan *prppFst = this->Prpp();
	CReqdPropPlan *prppSnd = poc->Prpp();
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
BOOL COptimizationContext::FEqualForStats(const COptimizationContext *pocLeft, const COptimizationContext *pocRight)
{
	GPOS_ASSERT(NULL != pocLeft);
	GPOS_ASSERT(NULL != pocRight);
	return pocLeft->GetReqdRelationalProps()->PcrsStat()->Equals(pocRight->GetReqdRelationalProps()->PcrsStat()) &&
		   pocLeft->Pdrgpstat()->Equals(pocRight->Pdrgpstat()) &&
		   pocLeft->Prpp()->Pepp()->PpfmDerived()->Equals(pocRight->Prpp()->Pepp()->PpfmDerived());
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
BOOL COptimizationContext::FOptimize(CMemoryPool *mp, CGroupExpression *pgexprParent, CGroupExpression *pgexprChild, COptimizationContext *pocChild, ULONG ulSearchStages)
{
	COperator *pop = pgexprChild->Pop();
	if (CUtils::FPhysicalMotion(pop))
	{
		return FOptimizeMotion(mp, pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}
	if (COperator::EopPhysicalSort == pop->Eopid())
	{
		return FOptimizeSort(mp, pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}
	if (CUtils::FPhysicalAgg(pop))
	{
		return FOptimizeAgg(mp, pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}
	if (CUtils::FNLJoin(pop))
	{
		return FOptimizeNLJoin(mp, pgexprParent, pgexprChild, pocChild, ulSearchStages);
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
BOOL COptimizationContext::FEqualContextIds(COptimizationContextArray *pdrgpocFst, COptimizationContextArray *pdrgpocSnd)
{
	if (NULL == pdrgpocFst || NULL == pdrgpocSnd)
	{
		return (NULL == pdrgpocFst && NULL == pdrgpocSnd);
	}
	const ULONG ulCtxts = pdrgpocFst->Size();
	if (ulCtxts != pdrgpocSnd->Size())
	{
		return false;
	}
	BOOL fEqual = true;
	for (ULONG ul = 0; fEqual && ul < ulCtxts; ul++)
	{
		fEqual = (*pdrgpocFst)[ul]->Id() == (*pdrgpocSnd)[ul]->Id();
	}
	return fEqual;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeMotion
//
//	@doc:
//		Check if a Motion node should be optimized for the given context
//
//---------------------------------------------------------------------------
BOOL COptimizationContext::FOptimizeMotion(CMemoryPool* mp, CGroupExpression* pgexprParent, CGroupExpression *pgexprMotion, COptimizationContext *poc, ULONG	ulSearchStages)
{
	GPOS_ASSERT(NULL != pgexprMotion);
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(CUtils::FPhysicalMotion(pgexprMotion->Pop()));
	CPhysicalMotion *pop = CPhysicalMotion::PopConvert(pgexprMotion->Pop());
	return poc->Prpp()->Ped()->FCompatible(pop->Pds());
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeSort
//
//	@doc:
//		Check if a Sort node should be optimized for the given context
//
//---------------------------------------------------------------------------
BOOL COptimizationContext::FOptimizeSort(CMemoryPool* mp, CGroupExpression* pgexprParent, CGroupExpression* pgexprSort, COptimizationContext* poc, ULONG ulSearchStages)
{
	GPOS_ASSERT(NULL != pgexprSort);
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(COperator::EopPhysicalSort == pgexprSort->Pop()->Eopid());
	CPhysicalSort *pop = CPhysicalSort::PopConvert(pgexprSort->Pop());
	return poc->Prpp()->Peo()->FCompatible(const_cast<COrderSpec *>(pop->Pos()));
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeAgg
//
//	@doc:
//		Check if Agg node should be optimized for the given context
//
//---------------------------------------------------------------------------
BOOL COptimizationContext::FOptimizeAgg(CMemoryPool* mp, CGroupExpression* pgexprParent, CGroupExpression* pgexprAgg, COptimizationContext *poc, ULONG ulSearchStages)
{
	GPOS_ASSERT(NULL != pgexprAgg);
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(CUtils::FPhysicalAgg(pgexprAgg->Pop()));
	GPOS_ASSERT(0 < ulSearchStages);
	if (GPOS_FTRACE(EopttraceForceExpandedMDQAs))
	{
		BOOL fHasMultipleDistinctAggs = CDrvdPropScalar::GetDrvdScalarProps((*pgexprAgg)[1]->Pdp())->HasMultipleDistinctAggs();
		if (fHasMultipleDistinctAggs)
		{
			// do not optimize plans with MDQAs, since preference is for plans with expanded MDQAs
			return false;
		}
	}
	if (!GPOS_FTRACE(EopttraceForceMultiStageAgg))
	{
		// no preference for multi-stage agg, we always proceed with optimization
		return true;
	}
	// otherwise, we need to avoid optimizing node unless it is a multi-stage agg
	COptimizationContext *pocFound = pgexprAgg->Pgroup()->PocLookupBest(mp, ulSearchStages, poc->Prpp());
	if (NULL != pocFound && pocFound->FHasMultiStageAggPlan())
	{
		// context already has a multi-stage agg plan, optimize child only if it is also a multi-stage agg
		return CPhysicalAgg::PopConvert(pgexprAgg->Pop())->FMultiStage();
	}
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
BOOL COptimizationContext::FOptimizeNLJoin(CMemoryPool *mp, CGroupExpression* pgexprParent, CGroupExpression *pgexprJoin, COptimizationContext *poc, ULONG ulSearchStages)
{
	GPOS_ASSERT(NULL != pgexprJoin);
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(CUtils::FNLJoin(pgexprJoin->Pop()));
	COperator *pop = pgexprJoin->Pop();
	if (!CUtils::FCorrelatedNLJoin(pop))
	{
		return true;
	}
	// For correlated join, the requested columns must be covered by outer child
	// columns and columns to be generated from inner child
	CPhysicalNLJoin *popNLJoin = CPhysicalNLJoin::PopConvert(pop);
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, popNLJoin->PdrgPcrInner());
	CColRefSet *pcrsOuterChild = CDrvdPropRelational::GetRelationalProperties((*pgexprJoin)[0]->Pdp())->GetOutputColumns();
	pcrs->Include(pcrsOuterChild);
	BOOL fIncluded = pcrs->ContainsAll(poc->Prpp()->PcrsRequired());
	pcrs->Release();
	return fIncluded;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::PrppCTEProducer
//
//	@doc:
//		Compute required properties to CTE producer based on plan properties
//		of CTE consumer
//
//---------------------------------------------------------------------------
CReqdPropPlan* COptimizationContext::PrppCTEProducer(CMemoryPool *mp, COptimizationContext *poc, ULONG ulSearchStages)
{
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT(NULL != poc->PccBest());
	CCostContext *pccBest = poc->PccBest();
	CGroupExpression *pgexpr = pccBest->Pgexpr();
	BOOL fOptimizeCTESequence = (COperator::EopPhysicalSequence == pgexpr->Pop()->Eopid() && (*pgexpr)[0]->FHasCTEProducer());
	if (!fOptimizeCTESequence)
	{
		// best group expression is not a CTE sequence
		return NULL;
	}
	COptimizationContext *pocProducer = (*pgexpr)[0]->PocLookupBest(mp, ulSearchStages, (*pccBest->Pdrgpoc())[0]->Prpp());
	if (NULL == pocProducer)
	{
		return NULL;
	}
	CCostContext *pccProducer = pocProducer->PccBest();
	if (NULL == pccProducer)
	{
		return NULL;
	}
	COptimizationContext *pocConsumer = (*pgexpr)[1]->PocLookupBest(mp, ulSearchStages, (*pccBest->Pdrgpoc())[1]->Prpp());
	if (NULL == pocConsumer)
	{
		return NULL;
	}
	CCostContext *pccConsumer = pocConsumer->PccBest();
	if (NULL == pccConsumer)
	{
		return NULL;
	}
	CColRefSet *pcrsInnerOutput = CDrvdPropRelational::GetRelationalProperties((*pgexpr)[1]->Pdp())->GetOutputColumns();
	CPhysicalCTEProducer *popProducer = CPhysicalCTEProducer::PopConvert(pccProducer->Pgexpr()->Pop());
	UlongToColRefMap *colref_mapping = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PhmulcrConsumerToProducer(mp, popProducer->UlCTEId(), pcrsInnerOutput, popProducer->Pdrgpcr());
	CReqdPropPlan *prppProducer = CReqdPropPlan::PrppRemapForCTE(mp, pocProducer->Prpp(), pccConsumer->Pdpplan(), colref_mapping);
	colref_mapping->Release();
	if (prppProducer->Equals(pocProducer->Prpp()))
	{
		prppProducer->Release();
		return NULL;
	}
	return prppProducer;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream& COptimizationContext::OsPrint(IOstream &os) const
{
	return OsPrintWithPrefix(os, "");
}

IOstream& COptimizationContext::OsPrintWithPrefix(IOstream &os, const CHAR *szPrefix) const
{
	os << szPrefix << m_id << " (stage " << m_ulSearchStageIndex << "): ("
	   << *m_prpp << ") => Best Expr:";
	if (NULL != PgexprBest())
	{
		os << PgexprBest()->Id();
	}
	os << std::endl;
	return os;
}