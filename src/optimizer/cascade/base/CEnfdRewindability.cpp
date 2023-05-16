//---------------------------------------------------------------------------
//	@filename:
//		CEnfdRewindability.cpp
//
//	@doc:
//		Implementation of rewindability property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CEnfdRewindability.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalSpool.h"

using namespace gpopt;

// initialization of static variables
const CHAR* CEnfdRewindability::m_szRewindabilityMatching[ErmSentinel] = {"satisfy"};

//---------------------------------------------------------------------------
//	@function:
//		CEnfdRewindability::CEnfdRewindability
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEnfdRewindability::CEnfdRewindability(CRewindabilitySpec *prs, ERewindabilityMatching erm)
	: m_prs(prs), m_erm(erm)
{
	GPOS_ASSERT(NULL != prs);
	GPOS_ASSERT(ErmSentinel > erm);
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdRewindability::~CEnfdRewindability
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEnfdRewindability::~CEnfdRewindability()
{
	CRefCount::SafeRelease(m_prs);
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdRewindability::FCompatible
//
//	@doc:
//		Check if the given rewindability specification is compatible
//		with the rewindability specification of this object for the
//		specified matching type
//
//---------------------------------------------------------------------------
BOOL
CEnfdRewindability::FCompatible(CRewindabilitySpec *prs) const
{
	GPOS_ASSERT(NULL != prs);

	switch (m_erm)
	{
		case ErmSatisfy:
			return prs->FSatisfies(m_prs);

		case ErmSentinel:
			GPOS_ASSERT("invalid matching type");
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdRewindability::HashValue
//
//	@doc:
// 		Hash function
//
//---------------------------------------------------------------------------
ULONG
CEnfdRewindability::HashValue() const
{
	return gpos::CombineHashes(m_erm + 1, m_prs->HashValue());
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdRewindability::Epet
//
//	@doc:
// 		Get rewindability enforcing type for the given operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CEnfdRewindability::Epet(CExpressionHandle &exprhdl, CPhysical *popPhysical,
						 BOOL fRewindabilityReqd) const
{
	if (fRewindabilityReqd)
	{
		return popPhysical->EpetRewindability(exprhdl, this);
	}

	return EpetUnnecessary;
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdRewindability::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CEnfdRewindability::OsPrint(IOstream &os) const
{
	(void) m_prs->OsPrint(os);

	return os << " match: " << m_szRewindabilityMatching[m_erm];
}