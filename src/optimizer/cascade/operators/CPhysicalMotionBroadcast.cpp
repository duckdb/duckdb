//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotionBroadcast.cpp
//
//	@doc:
//		Implementation of broadcast motion operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionBroadcast.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::CPhysicalMotionBroadcast
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcast::CPhysicalMotionBroadcast(CMemoryPool *mp)
	: CPhysicalMotion(mp), m_pdsReplicated(NULL)
{
	m_pdsReplicated = GPOS_NEW(mp) CDistributionSpecReplicated();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::~CPhysicalMotionBroadcast
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcast::~CPhysicalMotionBroadcast()
{
	m_pdsReplicated->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionBroadcast::Matches(COperator *pop) const
{
	return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionBroadcast::PcrsRequired(CMemoryPool *mp,
									   CExpressionHandle &exprhdl,
									   CColRefSet *pcrsRequired,
									   ULONG child_index,
									   CDrvdPropArray *,  // pdrgpdpCtxt
									   ULONG			  // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);

	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionBroadcast::FProvidesReqdCols(CExpressionHandle &exprhdl,
											CColRefSet *pcrsRequired,
											ULONG  // ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionBroadcast::EpetOrder(CExpressionHandle &,  // exprhdl
									const CEnfdOrder *	  // peo
) const
{
	// broadcast motion is not order-preserving
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionBroadcast::PosRequired(CMemoryPool *mp,
									  CExpressionHandle &,	// exprhdl
									  COrderSpec *,			//posInput
									  ULONG
#ifdef GPOS_DEBUG
										  child_index
#endif	// GPOS_DEBUG
									  ,
									  CDrvdPropArray *,	 // pdrgpdpCtxt
									  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// no order required from child expression
	return GPOS_NEW(mp) COrderSpec(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionBroadcast::PosDerive(CMemoryPool *mp,
									CExpressionHandle &	 // exprhdl
) const
{
	// broadcast motion is not order-preserving
	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionBroadcast::OsPrint(IOstream &os) const
{
	os << SzId() << " ";
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcast *
CPhysicalMotionBroadcast::PopConvert(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopPhysicalMotionBroadcast == pop->Eopid());

	return dynamic_cast<CPhysicalMotionBroadcast *>(pop);
}