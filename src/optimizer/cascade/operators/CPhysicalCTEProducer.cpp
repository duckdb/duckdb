//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalCTEProducer.cpp
//
//	@doc:
//		Implementation of CTE producer operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalCTEProducer.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCTEMap.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalSpool.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::CPhysicalCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalCTEProducer::CPhysicalCTEProducer(CMemoryPool *mp, ULONG id, CColRefArray *colref_array)
	: CPhysical(mp), m_id(id), m_pdrgpcr(colref_array), m_pcrs(NULL)
{
	GPOS_ASSERT(NULL != colref_array);
	m_pcrs = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::~CPhysicalCTEProducer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalCTEProducer::~CPhysicalCTEProducer()
{
	m_pdrgpcr->Release();
	m_pcrs->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalCTEProducer::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CColRefSet *pcrsRequired, ULONG child_index,
								   CDrvdPropArray *,  // pdrgpdpCtxt
								   ULONG			  // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(0 == pcrsRequired->Size());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrs);
	pcrs->Union(pcrsRequired);
	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);

	GPOS_ASSERT(pcrsChildReqd->Size() == m_pdrgpcr->Size());
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalCTEProducer::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  COrderSpec *posRequired, ULONG child_index,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PosPassThru(mp, exprhdl, posRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalCTEProducer::PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CDistributionSpec *pdsRequired,
								  ULONG child_index,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalCTEProducer::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CRewindabilitySpec *prsRequired,
								  ULONG child_index,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalCTEProducer::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CPartitionPropagationSpec *pppsRequired,
								   ULONG child_index,
								   CDrvdPropArray *,  //pdrgpdpCtxt,
								   ULONG			  //ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	return PppsRequiredPushThru(mp, exprhdl, pppsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalCTEProducer::PcteRequired(CMemoryPool *,		 //mp,
								   CExpressionHandle &,	 //exprhdl,
								   CCTEReq *pcter,
								   ULONG
#ifdef GPOS_DEBUG
									   child_index
#endif
								   ,
								   CDrvdPropArray *,  //pdrgpdpCtxt,
								   ULONG			  //ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalCTEProducer::PosDerive(CMemoryPool *,	// mp
								CExpressionHandle &exprhdl) const
{
	return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalCTEProducer::PdsDerive(CMemoryPool *,	// mp
								CExpressionHandle &exprhdl) const
{
	return PdsDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalCTEProducer::PrsDerive(CMemoryPool *mp,
								CExpressionHandle &exprhdl) const
{
	return PrsDerivePassThruOuter(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::PcmDerive
//
//	@doc:
//		Derive cte map
//
//---------------------------------------------------------------------------
CCTEMap *
CPhysicalCTEProducer::PcmDerive(CMemoryPool *mp,
								CExpressionHandle &exprhdl) const
{
	GPOS_ASSERT(1 == exprhdl.Arity());

	CCTEMap *pcmChild = exprhdl.Pdpplan(0)->GetCostModel();

	CCTEMap *pcmProducer = GPOS_NEW(mp) CCTEMap(mp);
	// store plan properties of the child in producer's CTE map
	pcmProducer->Insert(m_id, CCTEMap::EctProducer, exprhdl.Pdpplan(0));

	CCTEMap *pcmCombined = CCTEMap::PcmCombine(mp, *pcmProducer, *pcmChild);
	pcmProducer->Release();

	return pcmCombined;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalCTEProducer::FProvidesReqdCols(CExpressionHandle &exprhdl,
										CColRefSet *pcrsRequired,
										ULONG  // ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalCTEProducer::EpetOrder(CExpressionHandle &exprhdl,
								const CEnfdOrder *peo) const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();
	if (peo->FCompatible(pos))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalCTEProducer::EpetRewindability(CExpressionHandle &exprhdl,
										const CEnfdRewindability *per) const
{
	GPOS_ASSERT(NULL != per);

	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalCTEProducer::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CPhysicalCTEProducer *popCTEProducer =
		CPhysicalCTEProducer::PopConvert(pop);

	return m_id == popCTEProducer->UlCTEId() &&
		   m_pdrgpcr->Equals(popCTEProducer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalCTEProducer::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalCTEProducer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalCTEProducer::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]";

	return os;
}