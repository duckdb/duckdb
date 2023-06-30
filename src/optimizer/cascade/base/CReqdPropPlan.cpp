//---------------------------------------------------------------------------
//	@filename:
//		CReqdPropPlan.cpp
//
//	@doc:
//		Required plan properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CPrintablePointer.h"
#include "duckdb/optimizer/cascade/base/CCTEReq.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecAny.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecSingleton.h"
#include "duckdb/optimizer/cascade/base/CEnfdDistribution.h"
#include "duckdb/optimizer/cascade/base/CEnfdOrder.h"
#include "duckdb/optimizer/cascade/base/CEnfdPartitionPropagation.h"
#include "duckdb/optimizer/cascade/base/CEnfdRewindability.h"
#include "duckdb/optimizer/cascade/base/CPartFilterMap.h"
#include "duckdb/optimizer/cascade/base/CPartIndexMap.h"
#include "duckdb/optimizer/cascade/base/CPartInfo.h"
#include "duckdb/optimizer/cascade/base/CPartitionPropagationSpec.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"
#include "duckdb/optimizer/cascade/operators/CPhysical.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::CReqdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropPlan::CReqdPropPlan(CColRefSet *pcrs, CEnfdOrder *peo, CCTEReq *pcter)
	: m_pcrs(pcrs), m_peo(peo), m_pcter(pcter)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(NULL != ped);
	GPOS_ASSERT(NULL != per);
	GPOS_ASSERT(NULL != pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::~CReqdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdPropPlan::~CReqdPropPlan()
{
	CRefCount::SafeRelease(m_pcrs);
	CRefCount::SafeRelease(m_peo);
	CRefCount::SafeRelease(m_pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCols
//
//	@doc:
//		Compute required columns
//
//---------------------------------------------------------------------------
void CReqdPropPlan::ComputeReqdCols(CMemoryPool* mp, CExpressionHandle &exprhdl, CReqdProp* prpInput, ULONG child_index, CDrvdPropArray* pdrgpdpCtxt)
{
	GPOS_ASSERT(NULL == m_pcrs);
	CReqdPropPlan* prppInput = CReqdPropPlan::Prpp(prpInput);
	CPhysical* popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	m_pcrs = popPhysical->PcrsRequired(mp, exprhdl, prppInput->PcrsRequired(), child_index, pdrgpdpCtxt, 0);
}

void CReqdPropPlan::ComputeReqdCols(CMemoryPool* mp, PhysicalOperator* expr, CReqdProp* prpInput, ULONG child_index, CDrvdPropArray* pdrgpdpCtxt)
{
	GPOS_ASSERT(NULL == m_pcrs);
	CReqdPropPlan* prppInput = CReqdPropPlan::Prpp(prpInput);
	m_pcrs = expr->PcrsRequired(mp, prppInput->PcrsRequired(), child_index, pdrgpdpCtxt, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCTEs
//
//	@doc:
//		Compute required CTEs
//
//---------------------------------------------------------------------------
void CReqdPropPlan::ComputeReqdCTEs(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt)
{
	GPOS_ASSERT(NULL == m_pcter);
	CReqdPropPlan *prppInput = CReqdPropPlan::Prpp(prpInput);
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	m_pcter = popPhysical->PcteRequired(mp, exprhdl, prppInput->Pcter(), child_index, pdrgpdpCtxt, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void CReqdPropPlan::Compute(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
	GPOS_CHECK_ABORT;
	CReqdPropPlan *prppInput = CReqdPropPlan::Prpp(prpInput);
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	ComputeReqdCols(mp, exprhdl, prpInput, child_index, pdrgpdpCtxt);
	ComputeReqdCTEs(mp, exprhdl, prpInput, child_index, pdrgpdpCtxt);
	CPartFilterMap *ppfmDerived = PpfmCombineDerived(mp, exprhdl, prppInput, child_index, pdrgpdpCtxt);
	ULONG ulOrderReq = 0;
	ULONG ulDistrReq = 0;
	ULONG ulRewindReq = 0;
	ULONG ulPartPropagateReq = 0;
	popPhysical->LookupRequest(ulOptReq, &ulOrderReq, &ulDistrReq, &ulRewindReq, &ulPartPropagateReq);
	m_peo = GPOS_NEW(mp) CEnfdOrder(popPhysical->PosRequired(mp, exprhdl, prppInput->Peo()->PosRequired(), child_index, pdrgpdpCtxt, ulOrderReq), popPhysical->Eom(prppInput, child_index, pdrgpdpCtxt, ulOrderReq));
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PpfmCombineDerived
//
//	@doc:
//		Combine derived part filter map from input requirements and
//		derived plan properties in the passed context
//
//---------------------------------------------------------------------------
CPartFilterMap* CReqdPropPlan::PpfmCombineDerived(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdPropPlan *prppInput, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt)
{
	// get partitioning info below required child
	CPartInfo *ppartinfo = exprhdl.DerivePartitionInfo(child_index);
	const ULONG ulConsumers = ppartinfo->UlConsumers();

	CPartFilterMap *ppfmDerived = GPOS_NEW(mp) CPartFilterMap(mp);

	// a bit set of found scan id's with part filters
	CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp);

	// copy part filters from previously optimized children
	const ULONG size = pdrgpdpCtxt->Size();
	for (ULONG ulDrvdProps = 0; ulDrvdProps < size; ulDrvdProps++)
	{
		CDrvdPropPlan *pdpplan =
			CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[ulDrvdProps]);
		for (ULONG ul = 0; ul < ulConsumers; ul++)
		{
			ULONG scan_id = ppartinfo->ScanId(ul);
			BOOL fFound = pbs->Get(scan_id);

			if (!fFound)
			{
				BOOL fCopied = ppfmDerived->FCopyPartFilter(
					mp, scan_id, pdpplan->Ppfm(), NULL);
				if (fCopied)
				{
#ifdef GPOS_DEBUG
					BOOL fSet =
#endif	// GPOS_DEBUG
						pbs->ExchangeSet(scan_id);
					GPOS_ASSERT(!fSet);
				}
			}
		}
	}

	pbs->Release();

	return ppfmDerived;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Pps
//
//	@doc:
//		Given a property spec type, return the corresponding property spec
//		member
//
//---------------------------------------------------------------------------
CPropSpec* CReqdPropPlan::Pps(ULONG ul) const
{
	CPropSpec::EPropSpecType epst = (CPropSpec::EPropSpecType) ul;
	switch (epst)
	{
		case CPropSpec::EpstOrder:
			return m_peo->PosRequired();
		default:
			GPOS_ASSERT(!"Invalid property spec index");
	}
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Equals
//
//	@doc:
//		Check if expression attached to handle provides required columns
//		by all plan properties
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::FProvidesReqdCols(CMemoryPool* mp, CExpressionHandle &exprhdl, ULONG ulOptReq) const
{
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	// check if operator provides required columns
	if (!popPhysical->FProvidesReqdCols(exprhdl, m_pcrs, ulOptReq))
	{
		return false;
	}
	CColRefSet* pcrsOutput = exprhdl.DeriveOutputColumns();
	// check if property spec members use columns from operator output
	BOOL fProvidesReqdCols = true;
	for (ULONG ul = 0; fProvidesReqdCols && ul < CPropSpec::EpstSentinel; ul++)
	{
		CPropSpec *pps = Pps(ul);
		if (NULL == pps)
		{
			continue;
		}
		CColRefSet* pcrsUsed = pps->PcrsUsed(mp);
		fProvidesReqdCols = pcrsOutput->ContainsAll(pcrsUsed);
		pcrsUsed->Release();
	}
	return fProvidesReqdCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::Equals(const CReqdPropPlan *prpp) const
{
	GPOS_ASSERT(NULL != prpp);
	BOOL result = PcrsRequired()->Equals(prpp->PcrsRequired()) && Pcter()->Equals(prpp->Pcter()) && Peo()->Matches(prpp->Peo());
	return result;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::HashValue
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
ULONG CReqdPropPlan::HashValue() const
{
	GPOS_ASSERT(NULL != m_pcrs);
	GPOS_ASSERT(NULL != m_peo);
	GPOS_ASSERT(NULL != m_pcter);
	ULONG ulHash = m_pcrs->HashValue();
	ulHash = gpos::CombineHashes(ulHash, m_peo->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_pcter->HashValue());
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FSatisfied
//
//	@doc:
//		Check if plan properties are satisfied by the given derived properties
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::FSatisfied(const CDrvdPropRelational *pdprel, const CDrvdPropPlan *pdpplan) const
{
	GPOS_ASSERT(NULL != pdprel);
	GPOS_ASSERT(NULL != pdpplan);
	GPOS_ASSERT(pdprel->IsComplete());
	// first, check satisfiability of relational properties
	if (!pdprel->FSatisfies(this))
	{
		return false;
	}
	// second, check satisfiability of plan properties;
	// if max cardinality <= 1, then any order requirement is already satisfied;
	// we only need to check satisfiability of distribution and rewindability
	if (pdprel->GetMaxCard().Ull() <= 1)
	{
		GPOS_ASSERT(NULL != pdpplan->Ppim());
		return pdpplan->GetCostModel()->FSatisfies(this->Pcter());
	}
	// otherwise, check satisfiability of all plan properties
	return pdpplan->FSatisfies(this);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FCompatible
//
//	@doc:
//		Check if plan properties are compatible with the given derived properties
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::FCompatible(CExpressionHandle &exprhdl, CPhysical *popPhysical, const CDrvdPropRelational *pdprel, const CDrvdPropPlan *pdpplan) const
{
	GPOS_ASSERT(NULL != pdpplan);
	GPOS_ASSERT(NULL != pdprel);
	// first, check satisfiability of relational properties, including required columns
	if (!pdprel->FSatisfies(this))
	{
		return false;
	}
	return popPhysical->FProvidesReqdCTEs(exprhdl, m_pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppEmpty
//
//	@doc:
//		Generate empty required properties
//
//---------------------------------------------------------------------------
CReqdPropPlan* CReqdPropPlan::PrppEmpty(CMemoryPool *mp)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CCTEReq *pcter = GPOS_NEW(mp) CCTEReq(mp);
	return GPOS_NEW(mp) CReqdPropPlan(pcrs, peo, pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream & CReqdPropPlan::OsPrint(IOstream &os) const
{
	if (GPOS_FTRACE(EopttracePrintRequiredColumns))
	{
		os << "req cols: [";
		if (NULL != m_pcrs)
		{
			os << (*m_pcrs);
		}
		os << "], ";
	}
	os << "req CTEs: [";
	if (NULL != m_pcter)
	{
		os << (*m_pcter);
	}
	os << "], req order: [";
	if (NULL != m_peo)
	{
		os << (*m_peo);
	}
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::UlHashForCostBounding
//
//	@doc:
//		Hash function used for cost bounding
//
//---------------------------------------------------------------------------
ULONG CReqdPropPlan::UlHashForCostBounding(const CReqdPropPlan *prpp)
{
	GPOS_ASSERT(NULL != prpp);
	ULONG ulHash = prpp->PcrsRequired()->HashValue();
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FEqualForCostBounding
//
//	@doc:
//		Equality function used for cost bounding
//
//---------------------------------------------------------------------------
BOOL CReqdPropPlan::FEqualForCostBounding(const CReqdPropPlan *prppFst, const CReqdPropPlan *prppSnd)
{
	GPOS_ASSERT(NULL != prppFst);
	GPOS_ASSERT(NULL != prppSnd);
	return prppFst->PcrsRequired()->Equals(prppSnd->PcrsRequired());
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppRemap
//
//	@doc:
//		Map input required and derived plan properties into new required
//		plan properties for the CTE producer
//
//---------------------------------------------------------------------------
CReqdPropPlan* CReqdPropPlan::PrppRemapForCTE(CMemoryPool* mp, CReqdPropPlan* prppInput, CDrvdPropPlan* pdpplanInput, UlongToColRefMap* colref_mapping)
{
	GPOS_ASSERT(NULL != colref_mapping);
	GPOS_ASSERT(NULL != prppInput);
	GPOS_ASSERT(NULL != pdpplanInput);
	// Remap derived sort order to a required sort order.
	// a single order column, remap it to the equivalent CTE producer column
	COrderSpec* pos = pdpplanInput->Pos()->PosCopyWithRemappedColumns(mp, colref_mapping, false);
	CEnfdOrder* peo = GPOS_NEW(mp) CEnfdOrder(pos, prppInput->Peo()->Eom());
	// other properties are copied from input
	prppInput->PcrsRequired()->AddRef();
	CColRefSet* pcrsRequired = prppInput->PcrsRequired();
	prppInput->Pcter()->AddRef();
	CCTEReq* pcter = prppInput->Pcter();
	return GPOS_NEW(mp) CReqdPropPlan(pcrsRequired, peo, pcter);
}