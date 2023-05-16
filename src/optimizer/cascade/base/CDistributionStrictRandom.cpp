//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpecStrictRandom.cpp
//
//	@doc:
//		Class for representing forced random distribution.
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDistributionSpecStrictRandom.h"

using namespace gpopt;

CDistributionSpecStrictRandom::CDistributionSpecStrictRandom()
{
}

BOOL CDistributionSpecStrictRandom::Matches(const CDistributionSpec *pds) const
{
	return pds->Edt() == Edt();
}

BOOL CDistributionSpecStrictRandom::FSatisfies(const CDistributionSpec *pds) const
{
	return Matches(pds) || EdtAny == pds->Edt() || EdtRandom == pds->Edt() || EdtNonSingleton == pds->Edt();
}
