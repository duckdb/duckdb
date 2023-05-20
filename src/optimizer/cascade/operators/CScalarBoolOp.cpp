//---------------------------------------------------------------------------
//	@filename:
//		CScalarBoolOp.cpp
//
//	@doc:
//		Implementation of scalar boolean operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarBoolOp.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

const WCHAR CScalarBoolOp::m_rgwszBool[EboolopSentinel][30] = { GPOS_WSZ_LIT("EboolopAnd"), GPOS_WSZ_LIT("EboolopOr"), GPOS_WSZ_LIT("EboolopNot")};

//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		id of comparison
//
//---------------------------------------------------------------------------
ULONG CScalarBoolOp::HashValue() const
{
	ULONG ulBoolop = (ULONG) Eboolop();
	return gpos::CombineHashes(COperator::HashValue(), gpos::HashValue<ULONG>(&ulBoolop));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL CScalarBoolOp::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarBoolOp *popLog = CScalarBoolOp::PopConvert(pop);
		// match if operators are identical
		return Eboolop() == popLog->Eboolop();
	}
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::FCommutative
//
//	@doc:
//		Is boolean operator commutative?
//
//---------------------------------------------------------------------------
BOOL
CScalarBoolOp::FCommutative(EBoolOperator eboolop)
{
	switch (eboolop)
	{
		case EboolopAnd:
		case EboolopOr:
			return true;

		case EboolopNot:

		default:
			return false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarBoolOp::MdidType() const
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	return md_accessor->PtMDType<IMDTypeBool>()->MDId();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarBoolOp::Eber(ULongPtrArray *pdrgpulChildren) const
{
	if (EboolopAnd == m_eboolop)
	{
		return EberConjunction(pdrgpulChildren);
	}

	if (EboolopOr == m_eboolop)
	{
		return EberDisjunction(pdrgpulChildren);
	}

	GPOS_ASSERT(EboolopNot == m_eboolop);
	GPOS_ASSERT(NULL != pdrgpulChildren);
	GPOS_ASSERT(1 == pdrgpulChildren->Size());

	EBoolEvalResult eber = (EBoolEvalResult) * ((*pdrgpulChildren)[0]);
	switch (eber)
	{
		case EberTrue:
			return EberFalse;

		case EberFalse:
			return EberTrue;

		case EberNull:
			return EberNull;

		case EberNotTrue:
		default:
			return EberAny;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream & CScalarBoolOp::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_rgwszBool[m_eboolop];
	os << ")";

	return os;
}