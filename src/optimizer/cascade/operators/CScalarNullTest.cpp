//---------------------------------------------------------------------------
//	@filename:
//		CScalarNullTest.cpp
//
//	@doc:
//		Implementation of scalar null test operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarNullTest.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL CScalarNullTest::Matches(COperator *pop) const
{
	return pop->Eopid() == Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId* CScalarNullTest::MdidType() const
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	return md_accessor->PtMDType<IMDTypeBool>()->MDId();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult CScalarNullTest::Eber(ULongPtrArray* pdrgpulChildren) const
{
	GPOS_ASSERT(NULL != pdrgpulChildren);
	GPOS_ASSERT(1 == pdrgpulChildren->Size());
	EBoolEvalResult eber = (EBoolEvalResult) * ((*pdrgpulChildren)[0]);
	switch (eber)
	{
		case EberNull:
			return EberTrue;
		case EberFalse:
		case EberTrue:
			return EberFalse;
		case EberNotTrue:
		default:
			return EberAny;
	}
}