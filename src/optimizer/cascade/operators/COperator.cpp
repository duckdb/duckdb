//---------------------------------------------------------------------------
//	@filename:
//		COperator.cpp
//
//	@doc:
//		Implementation of operator base class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/COperator.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

// generate unique operator ids
ULONG COperator::m_aulOpIdCounter(0);

//---------------------------------------------------------------------------
//	@function:
//		COperator::COperator
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COperator::COperator(CMemoryPool *mp)
	: m_ulOpId(m_aulOpIdCounter++), m_mp(mp), m_fPattern(false)
{
	GPOS_ASSERT(NULL != mp);
}


//---------------------------------------------------------------------------
//	@function:
//		COperator::HashValue
//
//	@doc:
//		default hash function based on operator ID
//
//---------------------------------------------------------------------------
ULONG COperator::HashValue() const
{
	ULONG ulEopid = (ULONG) Eopid();

	return gpos::HashValue<ULONG>(&ulEopid);
}


//---------------------------------------------------------------------------
//	@function:
//		COperator::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
COperator::OsPrint(IOstream &os) const
{
	os << this->SzId();
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::EfdaDeriveFromChildren
//
//	@doc:
//		Derive data access function property from child expressions
//
//---------------------------------------------------------------------------
IMDFunction::EFuncDataAcc
COperator::EfdaDeriveFromChildren(CExpressionHandle &exprhdl,
								  IMDFunction::EFuncDataAcc efdaDefault)
{
	IMDFunction::EFuncDataAcc efda = efdaDefault;

	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		IMDFunction::EFuncDataAcc efdaChild = exprhdl.PfpChild(ul)->Efda();
		if (efdaChild > efda)
		{
			efda = efdaChild;
		}
	}

	return efda;
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::EfsDeriveFromChildren
//
//	@doc:
//		Derive stability function property from child expressions
//
//---------------------------------------------------------------------------
IMDFunction::EFuncStbl
COperator::EfsDeriveFromChildren(CExpressionHandle &exprhdl,
								 IMDFunction::EFuncStbl efsDefault)
{
	IMDFunction::EFuncStbl efs = efsDefault;

	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		IMDFunction::EFuncStbl efsChild = exprhdl.PfpChild(ul)->Efs();
		if (efsChild > efs)
		{
			efs = efsChild;
		}
	}

	return efs;
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::PfpDeriveFromChildren
//
//	@doc:
//		Derive function properties from child expressions
//
//---------------------------------------------------------------------------
CFunctionProp *
COperator::PfpDeriveFromChildren(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 IMDFunction::EFuncStbl efsDefault,
								 IMDFunction::EFuncDataAcc efdaDefault,
								 BOOL fHasVolatileFunctionScan, BOOL fScan)
{
	IMDFunction::EFuncStbl efs = EfsDeriveFromChildren(exprhdl, efsDefault);
	IMDFunction::EFuncDataAcc efda =
		EfdaDeriveFromChildren(exprhdl, efdaDefault);

	return GPOS_NEW(mp) CFunctionProp(
		efs, efda,
		fHasVolatileFunctionScan || exprhdl.FChildrenHaveVolatileFuncScan(),
		fScan);
}

//---------------------------------------------------------------------------
//	@function:
//		COperator::PopCopyDefault
//
//	@doc:
//		Return an addref'ed copy of the operator
//
//---------------------------------------------------------------------------
COperator* COperator::PopCopyDefault()
{
	this->AddRef();
	return this;
}