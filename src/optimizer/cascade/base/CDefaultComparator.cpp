//---------------------------------------------------------------------------
//	@filename:
//		CDefaultComparator.cpp
//
//	@doc:
//		Default comparator for IDatum instances to be used in constraint derivation
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDefaultComparator.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/eval/IConstExprEvaluator.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/base/IDatum.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

using namespace gpopt;
using namespace gpos;
using gpnaucrates::IDatum;

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::CDefaultComparator
//
//	@doc:
//		Ctor
//		Does not take ownership of the constant expression evaluator
//
//---------------------------------------------------------------------------
CDefaultComparator::CDefaultComparator(IConstExprEvaluator *pceeval)
	: m_pceeval(pceeval)
{
	GPOS_ASSERT(NULL != pceeval);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::PexprEvalComparison
//
//	@doc:
//		Constructs a comparison expression of type cmp_type between the two given
//		data and evaluates it.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::FEvalComparison(CMemoryPool *mp, const IDatum *datum1,
									const IDatum *datum2,
									IMDType::ECmpType cmp_type) const
{
	GPOS_ASSERT(m_pceeval->FCanEvalExpressions());

	IDatum *pdatum1Copy = datum1->MakeCopy(mp);
	CExpression *pexpr1 = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, pdatum1Copy));
	IDatum *pdatum2Copy = datum2->MakeCopy(mp);
	CExpression *pexpr2 = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, pdatum2Copy));
	CExpression *pexprComp =
		CUtils::PexprScalarCmp(mp, pexpr1, pexpr2, cmp_type);

	CExpression *pexprResult = m_pceeval->PexprEval(pexprComp);
	pexprComp->Release();
	CScalarConst *popScalarConst = CScalarConst::PopConvert(pexprResult->Pop());
	IDatum *datum = popScalarConst->GetDatum();

	GPOS_ASSERT(IMDType::EtiBool == datum->GetDatumType());
	IDatumBool *pdatumBool = dynamic_cast<IDatumBool *>(datum);
	BOOL result = pdatumBool->GetValue();
	pexprResult->Release();

	return result;
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::Equals
//
//	@doc:
//		Tests if the two arguments are equal.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::Equals(const IDatum *datum1, const IDatum *datum2) const
{
	if (!CUtils::FConstrainableType(datum1->MDId()) ||
		!CUtils::FConstrainableType(datum2->MDId()))
	{
		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(datum1->MDId()) &&
		CUtils::FIntType(datum2->MDId()))
	{
		return datum1->StatsAreEqual(datum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (datum1->IsNull() && datum2->IsNull())
	{
		return true;
	}

	return FEvalComparison(amp.Pmp(), datum1, datum2, IMDType::EcmptEq);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::IsLessThan
//
//	@doc:
//		Tests if the first argument is less than the second.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::IsLessThan(const IDatum *datum1, const IDatum *datum2) const
{
	if (!CUtils::FConstrainableType(datum1->MDId()) ||
		!CUtils::FConstrainableType(datum2->MDId()))
	{
		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(datum1->MDId()) &&
		CUtils::FIntType(datum2->MDId()))
	{
		return datum1->StatsAreLessThan(datum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (datum1->IsNull() && !datum2->IsNull())
	{
		return true;
	}

	return FEvalComparison(amp.Pmp(), datum1, datum2, IMDType::EcmptL);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::IsLessThanOrEqual
//
//	@doc:
//		Tests if the first argument is less than or equal to the second.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::IsLessThanOrEqual(const IDatum *datum1,
									  const IDatum *datum2) const
{
	if (!CUtils::FConstrainableType(datum1->MDId()) ||
		!CUtils::FConstrainableType(datum2->MDId()))
	{
		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(datum1->MDId()) &&
		CUtils::FIntType(datum2->MDId()))
	{
		return datum1->StatsAreLessThan(datum2) ||
			   datum1->StatsAreEqual(datum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (datum1->IsNull())
	{
		// return true since either:
		// 1. datum1 is NULL and datum2 is not NULL. Since NULL is considered
		// less that not null values for interval computations we return true
		// 2. when datum1 and datum2 are both NULL so they are equal
		// for interval computation

		return true;
	}


	return FEvalComparison(amp.Pmp(), datum1, datum2, IMDType::EcmptLEq);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::IsGreaterThan
//
//	@doc:
//		Tests if the first argument is greater than the second.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::IsGreaterThan(const IDatum *datum1,
								  const IDatum *datum2) const
{
	if (!CUtils::FConstrainableType(datum1->MDId()) ||
		!CUtils::FConstrainableType(datum2->MDId()))
	{
		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(datum1->MDId()) &&
		CUtils::FIntType(datum2->MDId()))
	{
		return datum1->StatsAreGreaterThan(datum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (!datum1->IsNull() && datum2->IsNull())
	{
		return true;
	}

	return FEvalComparison(amp.Pmp(), datum1, datum2, IMDType::EcmptG);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::IsLessThanOrEqual
//
//	@doc:
//		Tests if the first argument is greater than or equal to the second.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::IsGreaterThanOrEqual(const IDatum *datum1,
										 const IDatum *datum2) const
{
	if (!CUtils::FConstrainableType(datum1->MDId()) ||
		!CUtils::FConstrainableType(datum2->MDId()))
	{
		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(datum1->MDId()) &&
		CUtils::FIntType(datum2->MDId()))
	{
		return datum1->StatsAreGreaterThan(datum2) ||
			   datum1->StatsAreEqual(datum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (datum2->IsNull())
	{
		// return true since either:
		// 1. datum2 is NULL and datum1 is not NULL. Since NULL is considered
		// less that not null values for interval computations we return true
		// 2. when datum1 and datum2 are both NULL so they are equal
		// for interval computation
		return true;
	}

	return FEvalComparison(amp.Pmp(), datum1, datum2, IMDType::EcmptGEq);
}