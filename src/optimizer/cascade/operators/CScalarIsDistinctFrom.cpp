//---------------------------------------------------------------------------
//	@filename:
//		CScalarIsDistinctFrom.cpp
//
//	@doc:
//		Implementation of scalar IDF comparison operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarIsDistinctFrom.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"

using namespace gpopt;
using namespace gpmd;

// conversion function
CScalarIsDistinctFrom* CScalarIsDistinctFrom::PopConvert(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopScalarIsDistinctFrom == pop->Eopid());
	return reinterpret_cast<CScalarIsDistinctFrom *>(pop);
}

// perform boolean expression evaluation
CScalar::EBoolEvalResult CScalarIsDistinctFrom::Eber(ULongPtrArray *pdrgpulChildren) const
{
	GPOS_ASSERT(2 == pdrgpulChildren->Size());
	// Is Distinct From(IDF) expression will always evaluate
	// to a true/false/unknown but not a NULL
	EBoolEvalResult firstResult = (EBoolEvalResult) * (*pdrgpulChildren)[0];
	EBoolEvalResult secondResult = (EBoolEvalResult) * (*pdrgpulChildren)[1];
	if (firstResult == EberAny || secondResult == EberAny ||
		firstResult == EberNotTrue || secondResult == EberNotTrue)
	{
		return CScalar::EberAny;
	}
	else if (firstResult != secondResult)
	{
		return CScalar::EberTrue;
	}
	return CScalar::EberFalse;
}

BOOL CScalarIsDistinctFrom::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarIsDistinctFrom *popIDF = CScalarIsDistinctFrom::PopConvert(pop);
		// match if operator mdids are identical
		return MdIdOp()->Equals(popIDF->MdIdOp());
	}
	return false;
}

// get commuted scalar IDF operator
CScalarIsDistinctFrom* CScalarIsDistinctFrom::PopCommutedOp(CMemoryPool *mp, COperator *pop)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *mdid = PmdidCommuteOp(md_accessor, pop);
	if (NULL != mdid && mdid->IsValid())
	{
		return GPOS_NEW(mp) CScalarIsDistinctFrom(mp, mdid, Pstr(mp, md_accessor, mdid));
	}
	return NULL;
}