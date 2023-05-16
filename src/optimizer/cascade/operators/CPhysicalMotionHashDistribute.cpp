//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotionHashDistribute.cpp
//
//	@doc:
//		Implementation of hash distribute motion operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionHashDistribute.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecHashedNoOp.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::CPhysicalMotionHashDistribute
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistribute::CPhysicalMotionHashDistribute(CMemoryPool *mp, CDistributionSpecHashed *pdsHashed)
	: CPhysicalMotion(mp), m_pdsHashed(pdsHashed), m_pcrsRequiredLocal(NULL)
{
	GPOS_ASSERT(NULL != pdsHashed);
	GPOS_ASSERT(0 != pdsHashed->Pdrgpexpr()->Size());
	m_pcrsRequiredLocal = m_pdsHashed->PcrsUsed(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::~CPhysicalMotionHashDistribute
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistribute::~CPhysicalMotionHashDistribute()
{
	m_pdsHashed->Release();
	m_pcrsRequiredLocal->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionHashDistribute::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalMotionHashDistribute *popHashDistribute =
		CPhysicalMotionHashDistribute::PopConvert(pop);

	return m_pdsHashed->Equals(popHashDistribute->m_pdsHashed);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionHashDistribute::PcrsRequired(CMemoryPool *mp,
											CExpressionHandle &exprhdl,
											CColRefSet *pcrsRequired,
											ULONG child_index,
											CDrvdPropArray *,  // pdrgpdpCtxt
											ULONG			   // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrsRequiredLocal);
	pcrs->Union(pcrsRequired);
	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionHashDistribute::FProvidesReqdCols(CExpressionHandle &exprhdl,
												 CColRefSet *pcrsRequired,
												 ULONG	// ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionHashDistribute::EpetOrder(CExpressionHandle &,  // exprhdl
										 const CEnfdOrder *	   // peo
) const
{
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionHashDistribute::PosRequired(CMemoryPool *mp,
										   CExpressionHandle &,	 // exprhdl
										   COrderSpec *,		 //posInput
										   ULONG
#ifdef GPOS_DEBUG
											   child_index
#endif	// GPOS_DEBUG
										   ,
										   CDrvdPropArray *,  // pdrgpdpCtxt
										   ULONG			  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionHashDistribute::PosDerive(CMemoryPool *mp,
										 CExpressionHandle &  // exprhdl
) const
{
	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionHashDistribute::OsPrint(IOstream &os) const
{
	os << SzId() << " ";

	// choose a prefix big enough to avoid overlapping at least the simpler
	// expression trees
	return m_pdsHashed->OsPrintWithPrefix(
		os, "                                        ");
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistribute *
CPhysicalMotionHashDistribute::PopConvert(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopPhysicalMotionHashDistribute == pop->Eopid());

	return dynamic_cast<CPhysicalMotionHashDistribute *>(pop);
}

CDistributionSpec *
CPhysicalMotionHashDistribute::PdsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl, CDistributionSpec *pdsRequired,
	ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
	CDistributionSpecHashedNoOp *pdsNoOp =
		dynamic_cast<CDistributionSpecHashedNoOp *>(m_pdsHashed);
	if (NULL == pdsNoOp)
	{
		return CPhysicalMotion::PdsRequired(mp, exprhdl, pdsRequired,
											child_index, pdrgpdpCtxt, ulOptReq);
	}
	else
	{
		CExpressionArray *pdrgpexpr = pdsNoOp->Pdrgpexpr();
		pdrgpexpr->AddRef();
		CDistributionSpecHashed *pdsHashed = GPOS_NEW(mp)
			CDistributionSpecHashed(pdrgpexpr, pdsNoOp->FNullsColocated());
		return pdsHashed;
	}
}