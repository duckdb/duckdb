//---------------------------------------------------------------------------
//	@filename:
//		CScalarConst.cpp
//
//	@doc:
//		Implementation of scalar constant operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarConst.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/base/IDatumBool.h"

using namespace gpopt;
using namespace gpnaucrates;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::CScalarConst
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarConst::CScalarConst(CMemoryPool *mp, IDatum *datum)
	: CScalar(mp), m_pdatum(datum)
{
	GPOS_ASSERT(NULL != datum);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::~CScalarConst
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarConst::~CScalarConst()
{
	m_pdatum->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		hash of constant value
//
//---------------------------------------------------------------------------
ULONG
CScalarConst::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(), m_pdatum->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarConst::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarConst *psconst = CScalarConst::PopConvert(pop);

		// match if constant values are the same
		return GetDatum()->Matches(psconst->GetDatum());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarConst::MdidType() const
{
	return m_pdatum->MDId();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarConst::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	m_pdatum->OsPrint(os);
	os << ")";
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::FCastedConst
//
//	@doc:
// 		Is the given expression a cast of a constant expression
//
//---------------------------------------------------------------------------
BOOL
CScalarConst::FCastedConst(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	// cast(constant)
	if (COperator::EopScalarCast == pexpr->Pop()->Eopid())
	{
		if (COperator::EopScalarConst == (*pexpr)[0]->Pop()->Eopid())
		{
			return true;
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::PopExtractFromConstOrCastConst
//
//	@doc:
// 		Extract the constant from the given constant or a casted constant.
// 		Else return NULL.
//
//---------------------------------------------------------------------------
CScalarConst *
CScalarConst::PopExtractFromConstOrCastConst(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	BOOL fScConst = COperator::EopScalarConst == pexpr->Pop()->Eopid();
	BOOL fCastedScConst = CScalarConst::FCastedConst(pexpr);

	// constant or cast(constant)
	if (!fScConst && !fCastedScConst)
	{
		return NULL;
	}

	if (fScConst)
	{
		return CScalarConst::PopConvert(pexpr->Pop());
	}

	GPOS_ASSERT(fCastedScConst);

	return CScalarConst::PopConvert((*pexpr)[0]->Pop());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarConst::Eber(ULongPtrArray *	//pdrgpulChildren
) const
{
	if (m_pdatum->IsNull())
	{
		return EberNull;
	}

	if (IMDType::EtiBool == m_pdatum->GetDatumType())
	{
		IDatumBool *pdatumBool = dynamic_cast<IDatumBool *>(m_pdatum);
		if (pdatumBool->GetValue())
		{
			return EberTrue;
		}

		return EberFalse;
	}

	return EberAny;
}

INT CScalarConst::TypeModifier() const
{
	return m_pdatum->TypeModifier();
}