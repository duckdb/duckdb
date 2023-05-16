//---------------------------------------------------------------------------
//	@filename:
//		CScalar.cpp
//
//	@doc:
//		Implementation of base class of scalar operators
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalar.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalar::PdpCreate
//
//	@doc:
//		Create base container of derived properties
//
//---------------------------------------------------------------------------
CDrvdProp* CScalar::PdpCreate(CMemoryPool *mp) const
{
	return GPOS_NEW(mp) CDrvdPropScalar(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::PrpCreate
//
//	@doc:
//		Create base container of required properties
//
//---------------------------------------------------------------------------
CReqdProp *
CScalar::PrpCreate(CMemoryPool *  // mp
) const
{
	GPOS_ASSERT(!"Cannot compute required properties on scalar");
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::FHasSubquery
//
//	@doc:
//		Derive existence of subqueries from expression handle
//
//---------------------------------------------------------------------------
BOOL
CScalar::FHasSubquery(CExpressionHandle &exprhdl)
{
	// if operator is a subquery, return immediately
	if (CUtils::FSubquery(exprhdl.Pop()))
	{
		return true;
	}

	// otherwise, iterate over scalar children
	const ULONG arity = exprhdl.Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		if (exprhdl.FScalarChild(i))
		{
			if (exprhdl.DeriveHasSubquery(i))
			{
				return true;
			}
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberConjunction
//
//	@doc:
//		Perform conjunction of boolean evaluation results
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberConjunction(ULongPtrArray *pdrgpulChildren)
{
	GPOS_ASSERT(NULL != pdrgpulChildren);
	GPOS_ASSERT(1 < pdrgpulChildren->Size());

	// Here are the rules:
	// - if any child is FALSE, the result is FALSE
	// - else if all children are TRUE, the result is TRUE
	// - else if all children are ANY or TRUE, the result is ANY
	// - else if all children are NULL or TRUE, the result is NULL
	// - else (a mix of TRUE, ANY and NULL), the result is NotTrue
	BOOL fAllChildrenTrue = true;
	BOOL fAllChildrenAnyOrTrue = true;
	BOOL fAllChildrenNullOrTrue = true;

	const ULONG ulChildren = pdrgpulChildren->Size();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		EBoolEvalResult eber = (EBoolEvalResult) * ((*pdrgpulChildren)[ul]);

		if (EberFalse == eber)
		{
			return EberFalse;
		}

		fAllChildrenTrue = fAllChildrenTrue && (EberTrue == eber);
		fAllChildrenAnyOrTrue =
			fAllChildrenAnyOrTrue && (EberAny == eber || EberTrue == eber);
		fAllChildrenNullOrTrue =
			fAllChildrenNullOrTrue && (EberNull == eber || EberTrue == eber);
	}

	if (fAllChildrenTrue)
	{
		// conjunction of all True children gives True
		return EberTrue;
	}

	if (fAllChildrenAnyOrTrue)
	{
		return EberAny;
	}

	if (fAllChildrenNullOrTrue)
	{
		return EberNull;
	}

	return EberNotTrue;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberDisjunction
//
//	@doc:
//		Perform disjunction of child boolean evaluation results
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberDisjunction(ULongPtrArray *pdrgpulChildren)
{
	GPOS_ASSERT(NULL != pdrgpulChildren);
	GPOS_ASSERT(1 < pdrgpulChildren->Size());

	BOOL fAllChildrenFalse = true;
	BOOL fNullChild = false;
	BOOL fNotTrueChild = false;
	BOOL fAnyChild = false;

	const ULONG ulChildren = pdrgpulChildren->Size();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		EBoolEvalResult eber = (EBoolEvalResult) * ((*pdrgpulChildren)[ul]);
		switch (eber)
		{
			case EberTrue:
				// if any child is True, disjunction returns True
				return EberTrue;

			case EberNull:
				// if a child is NULL, we cannot return here yet since another child
				// might be True, which yields the disjunction to True
				fNullChild = true;
				break;

			case EberNotTrue:
				fNotTrueChild = true;
				break;

			case EberAny:
				fAnyChild = true;
				break;

			default:
				break;
		}

		fAllChildrenFalse = fAllChildrenFalse && (EberFalse == eber);
	}

	if (fAllChildrenFalse)
	{
		// disjunction of all False children gives False
		return EberFalse;
	}

	if (fAnyChild)
	{
		// at least one "any" child yields disjunction Unknown
		// for example,
		//   (Any OR False) = Any
		//   (Any OR Null) = Any (really NULL or TRUE, FALSE is excluded)

		return EberAny;
	}

	if (fNotTrueChild)
	{
		// a NotTrue child, with zero or more NULLs or FALSEs yields NotTrue
		//   (NotTrue OR False) = NotTrue
		//   (NotTrue OR Null) = NotTrue

		return EberNotTrue;
	}

	if (fNullChild)
	{
		// children are either False or Null, disjunction returns Null
		return EberNull;
	}

	return EberAny;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberNullOnAnyNullChild
//
//	@doc:
//		Return Null if any child is Null, return Any otherwise
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberNullOnAnyNullChild(ULongPtrArray *pdrgpulChildren)
{
	GPOS_ASSERT(NULL != pdrgpulChildren);

	const ULONG ulChildren = pdrgpulChildren->Size();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		EBoolEvalResult eber = (EBoolEvalResult) * ((*pdrgpulChildren)[ul]);
		if (EberNull == eber)
		{
			return EberNull;
		}
	}

	return EberAny;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberNullOnAllNullChildren
//
//	@doc:
//		Return Null evaluation result if all children results are Null
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberNullOnAllNullChildren(ULongPtrArray *pdrgpulChildren)
{
	GPOS_ASSERT(NULL != pdrgpulChildren);

	const ULONG ulChildren = pdrgpulChildren->Size();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		EBoolEvalResult eber = (EBoolEvalResult) * ((*pdrgpulChildren)[ul]);
		if (EberNull != eber)
		{
			return EberAny;
		}
	}

	return EberNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::EberEvaluate
//
//	@doc:
//		Perform boolean evaluation of the given expression
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalar::EberEvaluate(CMemoryPool *mp, CExpression *pexprScalar)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexprScalar);

	COperator *pop = pexprScalar->Pop();
	GPOS_ASSERT(pop->FScalar());

	const ULONG arity = pexprScalar->Arity();
	ULongPtrArray *pdrgpulChildren = NULL;

	if (!CUtils::FSubquery(pop))
	{
		// do not recurse into subqueries
		if (0 < arity)
		{
			pdrgpulChildren = GPOS_NEW(mp) ULongPtrArray(mp);
		}
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *pexprChild = (*pexprScalar)[ul];
			EBoolEvalResult eberChild = EberEvaluate(mp, pexprChild);
			pdrgpulChildren->Append(GPOS_NEW(mp) ULONG(eberChild));
		}
	}

	EBoolEvalResult eber = PopConvert(pop)->Eber(pdrgpulChildren);
	CRefCount::SafeRelease(pdrgpulChildren);

	return eber;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::FHasNonScalarFunction
//
//	@doc:
//		Derive existence of non-scalar functions from expression handle
//
//---------------------------------------------------------------------------
BOOL
CScalar::FHasNonScalarFunction(CExpressionHandle &exprhdl)
{
	// if operator is a subquery, return immediately
	if (CUtils::FSubquery(exprhdl.Pop()))
	{
		return false;
	}

	// otherwise, iterate over scalar children
	const ULONG arity = exprhdl.Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		if (exprhdl.FScalarChild(i))
		{
			if (exprhdl.DeriveHasNonScalarFunction(i))
			{
				return true;
			}
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalar::PdrgppartconDeriveCombineScalar
//
//	@doc:
//		Combine partition consumer arrays of all scalar children
//
//---------------------------------------------------------------------------
CPartInfo *
CScalar::PpartinfoDeriveCombineScalar(CMemoryPool *mp,
									  CExpressionHandle &exprhdl)
{
	const ULONG arity = exprhdl.Arity();
	GPOS_ASSERT(0 < arity);

	CPartInfo *ppartinfo = GPOS_NEW(mp) CPartInfo(mp);

	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (exprhdl.FScalarChild(ul))
		{
			CPartInfo *ppartinfoChild = exprhdl.DeriveScalarPartitionInfo(ul);
			GPOS_ASSERT(NULL != ppartinfoChild);
			CPartInfo *ppartinfoCombined =
				CPartInfo::PpartinfoCombine(mp, ppartinfo, ppartinfoChild);
			ppartinfo->Release();
			ppartinfo = ppartinfoCombined;
		}
	}

	return ppartinfo;
}

BOOL
CScalar::FHasScalarArrayCmp(CExpressionHandle &exprhdl)
{
	// if operator is a ScalarArrayCmp, return immediately
	if (COperator::EopScalarArrayCmp == exprhdl.Pop()->Eopid())
	{
		return true;
	}

	// otherwise, iterate over scalar children
	const ULONG arity = exprhdl.Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		if (exprhdl.FScalarChild(i))
		{
			if (exprhdl.DeriveHasScalarArrayCmp(i))
			{
				return true;
			}
		}
	}

	return false;
}