//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotionRandom.cpp
//
//	@doc:
//		Implementation of random motion operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionRandom.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::CPhysicalMotionRandom
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalMotionRandom::CPhysicalMotionRandom(CMemoryPool *mp, CDistributionSpecRandom *pdsRandom)
	: CPhysicalMotion(mp), m_pdsRandom(pdsRandom)
{
	GPOS_ASSERT(NULL != pdsRandom);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::~CPhysicalMotionRandom
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionRandom::~CPhysicalMotionRandom()
{
	m_pdsRandom->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionRandom::Matches(COperator *pop) const
{
	return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionRandom::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									CColRefSet *pcrsRequired, ULONG child_index,
									CDrvdPropArray *,  // pdrgpdpCtxt
									ULONG			   // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index,
						 gpos::ulong_max);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionRandom::FProvidesReqdCols(CExpressionHandle &exprhdl,
										 CColRefSet *pcrsRequired,
										 ULONG	// ulOptReq
) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionRandom::EpetOrder(CExpressionHandle &,  // exprhdl
								 const CEnfdOrder *
#ifdef GPOS_DEBUG
									 peo
#endif	// GPOS_DEBUG
) const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionRandom::PosRequired(CMemoryPool *mp,
								   CExpressionHandle &,	 //exprhdl,
								   COrderSpec *,		 //posInput,
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
//		CPhysicalMotionRandom::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionRandom::PosDerive(CMemoryPool *mp,
								 CExpressionHandle &  // exprhdl
) const
{
	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionRandom::OsPrint(IOstream &os) const
{
	os << SzId();

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionRandom::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalMotionRandom *
CPhysicalMotionRandom::PopConvert(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopPhysicalMotionRandom == pop->Eopid());

	return dynamic_cast<CPhysicalMotionRandom *>(pop);
}