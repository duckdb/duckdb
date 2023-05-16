//---------------------------------------------------------------------------
//	@filename:
//		CEnfdOrder.cpp
//
//	@doc:
//		Implementation of enforceable order property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CEnfdOrder.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalSort.h"

using namespace gpopt;

// initialization of static variables
const CHAR *CEnfdOrder::m_szOrderMatching[EomSentinel] = {"satisfy"};

//---------------------------------------------------------------------------
//	@function:
//		CEnfdOrder::CEnfdOrder
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEnfdOrder::CEnfdOrder(COrderSpec *pos, EOrderMatching eom)
	: m_pos(pos), m_eom(eom)
{
	GPOS_ASSERT(NULL != pos);
	GPOS_ASSERT(EomSentinel > eom);
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdOrder::~CEnfdOrder
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEnfdOrder::~CEnfdOrder()
{
	CRefCount::SafeRelease(m_pos);
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdOrder::FCompatible
//
//	@doc:
//		Check if the given order specification is compatible with the
//		order specification of this object for the specified matching type
//
//---------------------------------------------------------------------------
BOOL
CEnfdOrder::FCompatible(COrderSpec *pos) const
{
	GPOS_ASSERT(NULL != pos);

	switch (m_eom)
	{
		case EomSatisfy:
			return pos->FSatisfies(m_pos);

		case EomSentinel:
			GPOS_ASSERT("invalid matching type");
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdOrder::HashValue
//
//	@doc:
// 		Hash function
//
//---------------------------------------------------------------------------
ULONG
CEnfdOrder::HashValue() const
{
	return gpos::CombineHashes(m_eom + 1, m_pos->HashValue());
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdOrder::Epet
//
//	@doc:
// 		Get order enforcing type for the given operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CEnfdOrder::Epet(CExpressionHandle &exprhdl, CPhysical *popPhysical,
				 BOOL fOrderReqd) const
{
	if (fOrderReqd)
	{
		return popPhysical->EpetOrder(exprhdl, this);
	}
	return EpetUnnecessary;
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdOrder::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream& CEnfdOrder::OsPrint(IOstream &os) const
{
	return os << (*m_pos) << " match: " << m_szOrderMatching[m_eom] << " ";
}