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
#include "duckdb/execution/operator/order/physical_order.hpp"

namespace gpopt
{
using namespace duckdb;
using namespace gpos;

// initialization of static variables
const CHAR* CEnfdOrder::m_szOrderMatching[EomSentinel] = {"satisfy"};

//---------------------------------------------------------------------------
//	@function:
//		CEnfdOrder::CEnfdOrder
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEnfdOrder::CEnfdOrder(COrderSpec* pos, EOrderMatching eom)
	: m_pos(pos), m_eom(eom)
{
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
bool CEnfdOrder::FCompatible(COrderSpec* pos) const
{
	switch (m_eom)
	{
		case EomSatisfy:
			return pos->FSatisfies(m_pos);
		case EomSentinel:
			break;
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
ULONG CEnfdOrder::HashValue() const
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
CEnfdOrder::EPropEnforcingType CEnfdOrder::Epet(CExpressionHandle &exprhdl, PhysicalOperator* popPhysical, bool fOrderReqd) const
{
	if (fOrderReqd)
	{
		return popPhysical->EpetOrder(exprhdl, this->m_pos->m_pdrgpoe);
	}
	return EpetUnnecessary;
}
}