//---------------------------------------------------------------------------
//	@filename:
//		CDatumSortedSet.cpp
//
//	@doc:
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDatumSortedSet.h"
#include "duckdb/optimizer/cascade/common/CAutoRef.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/COperator.h"
#include "duckdb/optimizer/cascade/operators/CScalarConst.h"

using namespace gpopt;

CDatumSortedSet::CDatumSortedSet(CMemoryPool *mp, CExpression *pexprArray, const IComparator *pcomp)
	: IDatumArray(mp), m_fIncludesNull(false)
{
	GPOS_ASSERT(COperator::EopScalarArray == pexprArray->Pop()->Eopid());

	const ULONG ulArrayExprArity = CUtils::UlScalarArrayArity(pexprArray);
	GPOS_ASSERT(0 < ulArrayExprArity);

	gpos::CAutoRef<IDatumArray> aprngdatum(GPOS_NEW(mp) IDatumArray(mp));
	for (ULONG ul = 0; ul < ulArrayExprArity; ul++)
	{
		CScalarConst *popScConst = CUtils::PScalarArrayConstChildAt(pexprArray, ul);
		IDatum *datum = popScConst->GetDatum();
		if (datum->IsNull())
		{
			m_fIncludesNull = true;
		}
		else
		{
			datum->AddRef();
			aprngdatum->Append(datum);
		}
	}
	aprngdatum->Sort(&CUtils::IDatumCmp);

	// de-duplicate
	const ULONG ulRangeArrayArity = aprngdatum->Size();
	IDatum *pdatumPrev = (*aprngdatum)[0];
	pdatumPrev->AddRef();
	Append(pdatumPrev);
	for (ULONG ul = 1; ul < ulRangeArrayArity; ul++)
	{
		if (!pcomp->Equals((*aprngdatum)[ul], pdatumPrev))
		{
			pdatumPrev = (*aprngdatum)[ul];
			pdatumPrev->AddRef();
			Append(pdatumPrev);
		}
	}
}

BOOL CDatumSortedSet::FIncludesNull() const
{
	return m_fIncludesNull;
}