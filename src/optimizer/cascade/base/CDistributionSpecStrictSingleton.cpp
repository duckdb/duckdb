//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpecStrictSingleton.cpp
//
//	@doc:
//		Specification of strict singleton distribution
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDistributionSpecStrictSingleton.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecStrictSingleton::CDistributionSpecStrictSingleton
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecStrictSingleton::CDistributionSpecStrictSingleton(
	ESegmentType est)
	: CDistributionSpecSingleton(est)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecStrictSingleton::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecStrictSingleton::FSatisfies(
	const CDistributionSpec *pdss) const
{
	if (Matches(pdss))
	{
		// exact match implies satisfaction
		return true;
	}

	if (EdtNonSingleton == pdss->Edt())
	{
		// singleton does not satisfy non-singleton requirements
		return false;
	}

	if (EdtAny == pdss->Edt())
	{
		// a singleton distribution satisfies "any"
		return true;
	}

	return (
		(EdtSingleton == pdss->Edt() || EdtStrictSingleton == pdss->Edt()) &&
		m_est == ((CDistributionSpecStrictSingleton *) pdss)->Est());
}



//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecStrictSingleton::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecStrictSingleton::OsPrint(IOstream &os) const
{
	return os << "STRICT SINGLETON (" << m_szSegmentType[m_est] << ")";
}