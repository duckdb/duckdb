//---------------------------------------------------------------------------
//	@filename:
//		CScalarNAryJoinPredList.cpp
//
//	@doc:
//		Join predicate list for NAry joins with some non-inner joins
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarNAryJoinPredList.h"
#include "duckdb/optimizer/cascade/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarNAryJoinPredList::CScalarNAryJoinPredList
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CScalarNAryJoinPredList::CScalarNAryJoinPredList(CMemoryPool *mp) : CScalar(mp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarNAryJoinPredList::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL CScalarNAryJoinPredList::Matches(COperator *pop) const
{
	return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarNAryJoinPredList::FInputOrderSensitive
//
//	@doc:
//		Join predicate lists are sensitive to order
//
//---------------------------------------------------------------------------
BOOL CScalarNAryJoinPredList::FInputOrderSensitive() const
{
	return true;
}