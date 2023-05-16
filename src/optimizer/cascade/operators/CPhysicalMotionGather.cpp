//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotionGather.cpp
//
//	@doc:
//		Implementation of gather motion operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionGather.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::CPhysicalMotionGather
//
//	@doc:
//		Ctor: create a non order-preserving motion
//
//---------------------------------------------------------------------------
CPhysicalMotionGather::CPhysicalMotionGather(
	CMemoryPool *mp, CDistributionSpecSingleton::ESegmentType est)
	: CPhysicalMotion(mp), m_pdssSingeton(NULL), m_pcrsSort(NULL)
{
	GPOS_ASSERT(CDistributionSpecSingleton::EstSentinel != est);

	m_pdssSingeton = GPOS_NEW(mp) CDistributionSpecSingleton(est);
	m_pos = GPOS_NEW(m_mp) COrderSpec(m_mp);
	m_pcrsSort = m_pos->PcrsUsed(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::CPhysicalMotionGather
//
//	@doc:
//		Ctor: create an order-preserving motion
//
//---------------------------------------------------------------------------
CPhysicalMotionGather::CPhysicalMotionGather(
	CMemoryPool *mp, CDistributionSpecSingleton::ESegmentType est,
	COrderSpec *pos)
	: CPhysicalMotion(mp), m_pdssSingeton(NULL), m_pos(pos), m_pcrsSort(NULL)
{
	GPOS_ASSERT(CDistributionSpecSingleton::EstSentinel != est);
	GPOS_ASSERT(NULL != pos);

	m_pdssSingeton = GPOS_NEW(mp) CDistributionSpecSingleton(est);
	m_pcrsSort = m_pos->PcrsUsed(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::~CPhysicalMotionGather
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionGather::~CPhysicalMotionGather()
{
	m_pos->Release();
	m_pdssSingeton->Release();
	m_pcrsSort->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionGather::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalMotionGather *popGather = CPhysicalMotionGather::PopConvert(pop);

	return Est() == popGather->Est() && m_pos->Matches(popGather->Pos());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionGather::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									CColRefSet *pcrsRequired, ULONG child_index,
									CDrvdPropArray *,  // pdrgpdpCtxt
									ULONG			   // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *m_pcrsSort);
	pcrs->Union(pcrsRequired);

	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionGather::FProvidesReqdCols(CExpressionHandle &exprhdl,
										 CColRefSet *pcrsRequired,
										 ULONG	// ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionGather::EpetOrder(CExpressionHandle &,  // exprhdl
								 const CEnfdOrder *peo) const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	if (!FOrderPreserving())
	{
		return CEnfdProp::EpetRequired;
	}

	if (peo->FCompatible(m_pos))
	{
		// required order is already established by gather merge operator
		return CEnfdProp::EpetUnnecessary;
	}

	// required order is incompatible with the order established by the
	// gather merge operator, prohibit adding another sort operator on top
	return CEnfdProp::EpetProhibited;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionGather::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   COrderSpec *,  //posInput,
								   ULONG child_index,
								   CDrvdPropArray *,  // pdrgpdpCtxt
								   ULONG			  // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	if (FOrderPreserving())
	{
		return PosPassThru(mp, exprhdl, m_pos, child_index);
	}

	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionGather::PosDerive(CMemoryPool *,		  // mp
								 CExpressionHandle &  // exprhdl
) const
{
	m_pos->AddRef();
	return m_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionGather::OsPrint(IOstream &os) const
{
	const CHAR *szLocation = FOnMaster() ? "(master)" : "(segment)";
	os << SzId() << szLocation;

	if (FOrderPreserving())
	{
		Pos()->OsPrint(os);
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionGather::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalMotionGather* CPhysicalMotionGather::PopConvert(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopPhysicalMotionGather == pop->Eopid());

	return dynamic_cast<CPhysicalMotionGather *>(pop);
}