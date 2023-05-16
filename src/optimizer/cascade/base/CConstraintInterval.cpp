//---------------------------------------------------------------------------
//	@filename:
//		CConstraintInterval.cpp
//
//	@doc:
//		Implementation of interval constraints
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CConstraintInterval.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CAutoRef.h"

#include "duckdb/optimizer/cascade/base/CCastUtils.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CConstraintDisjunction.h"
#include "duckdb/optimizer/cascade/base/CDatumSortedSet.h"
#include "duckdb/optimizer/cascade/base/CDefaultComparator.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/operators/CScalarArray.h"
#include "duckdb/optimizer/cascade/operators/CScalarIdent.h"
#include "duckdb/optimizer/cascade/operators/CScalarIsDistinctFrom.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::CConstraintInterval
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraintInterval::CConstraintInterval(CMemoryPool *mp, const CColRef *colref, CRangeArray *pdrgprng, BOOL fIncludesNull)
	: CConstraint(mp), m_pcr(colref), m_pdrgprng(pdrgprng), m_fIncludesNull(fIncludesNull)
{
	GPOS_ASSERT(NULL != colref);
	GPOS_ASSERT(NULL != pdrgprng);
	m_pcrsUsed = GPOS_NEW(mp) CColRefSet(mp);
	m_pcrsUsed->Include(colref);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::~CConstraintInterval
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraintInterval::~CConstraintInterval()
{
	m_pdrgprng->Release();
	m_pcrsUsed->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FContradiction
//
//	@doc:
//		Check if this constraint is a contradiction. An interval is a contradiction if
//		it has no ranges and the null flag is not set
//
//---------------------------------------------------------------------------
BOOL CConstraintInterval::FContradiction() const
{
	return (!m_fIncludesNull && 0 == m_pdrgprng->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::IsConstraintUnbounded
//
//	@doc:
//		Check if this interval is unbounded. An interval is unbounded if
//		it has a (-inf, inf) range and the null flag is set
//
//---------------------------------------------------------------------------
BOOL CConstraintInterval::IsConstraintUnbounded() const
{
	return (m_fIncludesNull && 1 == m_pdrgprng->Size() && (*m_pdrgprng)[0]->IsConstraintUnbounded());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the constraint with remapped columns
//
//---------------------------------------------------------------------------
CConstraint* CConstraintInterval::PcnstrCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRef *colref = CUtils::PcrRemap(m_pcr, colref_mapping, must_exist);
	return PcnstrRemapForColumn(mp, colref);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarExpr
//
//	@doc:
//		Create interval from scalar expression
//
//		For a given expression pexpr on colref colref, return the CConstraintInterval
//		for which pexpr
//		- evaluates to true (if infer_null_as is false).
//		  This is used for WHERE predicates, which return a row only if the predicate is true.
//		- evaluates to true or null (if infer_null_as is set to true).
//		  This is used for constraints, which are satisfied if the predicate is true or null.
//
//		Let's call the function result r(pexpr) when infer_null_as is set to false,
//		and r'(pexpr) when infer_null_as is set to true. The table below shows how we
//		calculate the intervals for boolean operations AND, OR and NOT:
//
//		Range of a			Equivalent				Comment
//		Boolean expression	expression
//		------------------	---------------------	--------------------------------------------------------
//		r(x and y)			r(x) intersect r(y)		Both x and y must be true for a value of c to qualify
//		r(x or y)			r(x) union r(y)			One of x or y must be true for a value of c to qualify
//		r(not x)			complement(r’(x))		x must be false
//		r’(x and y)			r’(x) intersect r’(y)	Both x and y must not be false
//		r’(x or y)			r'(x) union r'(y)		At least one of x and y must not be false
//		r’(not x)			complement (r(x))		x must not be true
//
//---------------------------------------------------------------------------
CConstraintInterval* CConstraintInterval::PciIntervalFromScalarExpr(CMemoryPool *mp, CExpression *pexpr, CColRef *colref, BOOL infer_nulls_as)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	// expression must use at most one column
	GPOS_ASSERT(1 >= pexpr->DeriveUsedColumns()->Size());
	CConstraintInterval *pci = NULL;
	switch (pexpr->Pop()->Eopid())
	{
		case COperator::EopScalarNullTest:
			pci = PciIntervalFromScalarNullTest(mp, pexpr, colref);
			break;
		case COperator::EopScalarBoolOp:
			pci =
				PciIntervalFromScalarBoolOp(mp, pexpr, colref, infer_nulls_as);
			break;
		case COperator::EopScalarCmp:
			pci = PciIntervalFromScalarCmp(mp, pexpr, colref, infer_nulls_as);
			break;
		case COperator::EopScalarIsDistinctFrom:
			pci = PciIntervalFromScalarIDF(mp, pexpr, colref);
			break;
		case COperator::EopScalarConst:
		{
			if (CUtils::FScalarConstTrue(pexpr))
			{
				pci = CConstraintInterval::PciUnbounded(mp, colref, true /*fIncludesNull*/);
			}
			else
			{
				pci = GPOS_NEW(mp) CConstraintInterval(mp, colref, GPOS_NEW(mp) CRangeArray(mp), false /*fIncludesNull*/);
			}
		}
		break;
		case COperator::EopScalarArrayCmp:
			if (GPOS_FTRACE(EopttraceArrayConstraints))
			{
				pci = CConstraintInterval::PcnstrIntervalFromScalarArrayCmp(mp, pexpr, colref, infer_nulls_as);
			}
			break;
		default:
			pci = NULL;
	}

	return pci;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrFromScalarArrayCmp
//
//	@doc:
//		Create constraint from scalar array comparison expression. Returns
//		NULL if a constraint interval cannot be created. Has side effect of
//		removing duplicates
//
//---------------------------------------------------------------------------
CConstraintInterval* CConstraintInterval::PcnstrIntervalFromScalarArrayCmp(CMemoryPool *mp, CExpression *pexpr, CColRef *colref, BOOL infer_nulls_as)
{
	if (!(CPredicateUtils::FCompareIdentToConstArray(pexpr) || CPredicateUtils::FCompareCastIdentToConstArray(pexpr)))
	{
		return NULL;
	}
#ifdef GPOS_DEBUG
	else
	{
		// verify column in expr is the same as column which was passed in
		CScalarIdent *popScId = NULL;
		if (CUtils::FScalarIdent((*pexpr)[0]))
		{
			popScId = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
		}
		else
		{
			GPOS_ASSERT(CScalarIdent::FCastedScId((*pexpr)[0]));
			popScId = CScalarIdent::PopConvert((*(*pexpr)[0])[0]->Pop());
		}
		GPOS_ASSERT(colref == (CColRef *) popScId->Pcr());
	}
#endif	// GPOS_DEBUG

	CScalarArrayCmp *popScArrayCmp = CScalarArrayCmp::PopConvert(pexpr->Pop());
	IMDType::ECmpType cmp_type = CUtils::ParseCmpType(popScArrayCmp->MdIdOp());


	CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
	const ULONG ulArrayExprArity = CUtils::UlScalarArrayArity(pexprArray);
	if (0 == ulArrayExprArity)
	{
		return NULL;
	}

	const IComparator *pcomp = COptCtxt::PoctxtFromTLS()->Pcomp();
	gpos::CAutoRef<CDatumSortedSet> apdatumsortedset(GPOS_NEW(mp) CDatumSortedSet(mp, pexprArray, pcomp));
	// construct ranges representing IN or NOT IN
	CRangeArray *prgrng = GPOS_NEW(mp) CRangeArray(mp);

	switch (cmp_type)
	{
		case IMDType::EcmptEq:
		{
			// IN case, create ranges [X, X] [Y, Y] [Z, Z]
			for (ULONG ul = 0; ul < apdatumsortedset->Size(); ul++)
			{
				(*apdatumsortedset)[ul]->AddRef();
				CRange *prng = GPOS_NEW(mp) CRange(pcomp, IMDType::EcmptEq, (*apdatumsortedset)[ul]);
				prgrng->Append(prng);
			}
			break;
		}
		case IMDType::EcmptNEq:
		{
			// NOT IN case, create ranges: (-inf, X) (X, Y) (Y, Z) (Z, inf)
			IDatum *pprevdatum = NULL;
			IDatum *datum = NULL;

			for (ULONG ul = 0; ul < apdatumsortedset->Size(); ul++)
			{
				if (0 != ul)
				{
					pprevdatum->AddRef();
				}

				datum = (*apdatumsortedset)[ul];
				datum->AddRef();

				IMDId *mdid = datum->MDId();
				mdid->AddRef();

				CRange *prng = GPOS_NEW(mp) CRange(mdid, pcomp, pprevdatum, CRange::EriExcluded, datum, CRange::EriExcluded);
				prgrng->Append(prng);

				pprevdatum = datum;
			}

			// add the last datum, making range (last, inf)
			IMDId *mdid = pprevdatum->MDId();
			pprevdatum->AddRef();
			mdid->AddRef();
			CRange *prng = GPOS_NEW(mp) CRange(mdid, pcomp, pprevdatum, CRange::EriExcluded, NULL, CRange::EriExcluded);
			prgrng->Append(prng);
			break;
		}
		default:
		{
			// does not handle IS DISTINCT FROM
			prgrng->Release();
			return NULL;
		}
	}
	return GPOS_NEW(mp) CConstraintInterval(mp, colref, prgrng, infer_nulls_as);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromConstraint
//
//	@doc:
//		Create interval from any general constraint that references only one column
//
//---------------------------------------------------------------------------
CConstraintInterval* CConstraintInterval::PciIntervalFromConstraint(CMemoryPool *mp, CConstraint *pcnstr, CColRef *colref)
{
	if (NULL == pcnstr)
	{
		GPOS_ASSERT(NULL != colref && "Must provide valid column reference to construct unbounded interval");
		return PciUnbounded(mp, colref, true /*fIncludesNull*/);
	}

	if (CConstraint::EctInterval == pcnstr->Ect())
	{
		pcnstr->AddRef();
		return dynamic_cast<CConstraintInterval *>(pcnstr);
	}

	CColRefSet *pcrsUsed = pcnstr->PcrsUsed();
	GPOS_ASSERT(1 == pcrsUsed->Size());

	CColRef *pcrFirst = pcrsUsed->PcrFirst();
	GPOS_ASSERT_IMP(NULL != colref, pcrFirst == colref);

	CExpression *pexprScalar = pcnstr->PexprScalar(mp);

	return PciIntervalFromScalarExpr(mp, pexprScalar, pcrFirst);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarNullTest
//
//	@doc:
//		Create interval from scalar null test
//
//---------------------------------------------------------------------------
CConstraintInterval* CConstraintInterval::PciIntervalFromScalarNullTest(CMemoryPool *mp, CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarNullTest(pexpr));

	// child of comparison operator
	CExpression *pexprChild = (*pexpr)[0];

	// TODO:  - May 28, 2012; add support for other expression forms
	// besides (ident is null)

	if (CUtils::FScalarIdent(pexprChild))
	{
#ifdef GPOS_DEBUG
		CScalarIdent *popScId = CScalarIdent::PopConvert(pexprChild->Pop());
		GPOS_ASSERT(colref == (CColRef *) popScId->Pcr());
#endif	// GPOS_DEBUG
		return GPOS_NEW(mp) CConstraintInterval(
			mp, colref, GPOS_NEW(mp) CRangeArray(mp), true /*fIncludesNull*/);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarCmp
//
//	@doc:
//		Helper for create interval from comparison between a column and
//		a constant
//
//---------------------------------------------------------------------------
CConstraintInterval* CConstraintInterval::PciIntervalFromColConstCmp(CMemoryPool *mp, CColRef *colref, IMDType::ECmpType cmp_type, CScalarConst *popScConst, BOOL infer_nulls_as)
{
	CConstraintInterval *pcri = NULL;
	CRangeArray *pdrngprng = PciRangeFromColConstCmp(mp, cmp_type, popScConst);

	if (NULL != pdrngprng)
	{
		// (col = const) usually implies (col IS NOT NULL) for these ops since
		// NULLs are inferred as false.  But, if asked to infer NULLS as true (e.g
		// in table constraints), include NULL in the final interval.
		pcri = GPOS_NEW(mp) CConstraintInterval(mp, colref, pdrngprng, infer_nulls_as);
	}
	return pcri;
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarCmp
//
//	@doc:
//		Create interval from scalar comparison expression
//
//---------------------------------------------------------------------------
CConstraintInterval* CConstraintInterval::PciIntervalFromScalarCmp(CMemoryPool *mp, CExpression *pexpr, CColRef *colref, BOOL infer_nulls_as)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarCmp(pexpr) || CUtils::FScalarArrayCmp(pexpr));

	// TODO:  - May 28, 2012; add support for other expression forms
	// besides (column relop const)
	if (CPredicateUtils::FCompareIdentToConst(pexpr))
	{
		// column
#ifdef GPOS_DEBUG
		CScalarIdent *popScId;
		CExpression *pexprLeft = (*pexpr)[0];
		if (CUtils::FScalarIdent((*pexpr)[0]))
		{
			popScId = CScalarIdent::PopConvert(pexprLeft->Pop());
		}
		else
		{
			GPOS_ASSERT(CCastUtils::FBinaryCoercibleCastedScId(pexprLeft));
			popScId = CScalarIdent::PopConvert((*pexprLeft)[0]->Pop());
		}
		GPOS_ASSERT(colref == (CColRef *) popScId->Pcr());
#endif	// GPOS_DEBUG

		// constant
		CExpression *pexprRight = (*pexpr)[1];
		CScalarConst *popScConst;
		if (CUtils::FScalarConst(pexprRight))
		{
			popScConst = CScalarConst::PopConvert(pexprRight->Pop());
		}
		else
		{
			GPOS_ASSERT(CCastUtils::FBinaryCoercibleCastedConst(pexprRight));
			popScConst = CScalarConst::PopConvert((*pexprRight)[0]->Pop());
		}
		CScalarCmp *popScCmp = CScalarCmp::PopConvert(pexpr->Pop());

		return PciIntervalFromColConstCmp(mp, colref, popScCmp->ParseCmpType(), popScConst, infer_nulls_as);
	}

	return NULL;
}

CConstraintInterval* CConstraintInterval::PciIntervalFromScalarIDF(CMemoryPool *mp, CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CPredicateUtils::FIDF(pexpr));

	if (CPredicateUtils::FIdentIDFConst(pexpr))
	{
		// column
#ifdef GPOS_DEBUG
		CScalarIdent *popScId = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
		GPOS_ASSERT(colref == (CColRef *) popScId->Pcr());
#endif	// GPOS_DEBUG

		// constant
		CScalarConst *popScConst = CScalarConst::PopConvert((*pexpr)[1]->Pop());
		// operator
		CScalarIsDistinctFrom *popScCmp = CScalarIsDistinctFrom::PopConvert(pexpr->Pop());

		GPOS_ASSERT(CScalar::EopScalarConst == popScConst->Eopid());
		GPOS_ASSERT(IMDType::EcmptIDF == popScCmp->ParseCmpType());

		IDatum *datum = popScConst->GetDatum();
		CConstraintInterval *pcri = NULL;

		if (datum->IsNull())
		{
			// col IS DISTINCT FROM NULL
			CConstraintInterval *pcriChild = GPOS_NEW(mp)
				CConstraintInterval(mp, colref, GPOS_NEW(mp) CRangeArray(mp), true /*fIncludesNull*/);
			pcri = pcriChild->PciComplement(mp);
			pcriChild->Release();
		}
		else
		{
			// col IS DISTINCT FROM const
			CRangeArray *pdrgprng = PciRangeFromColConstCmp(mp, popScCmp->ParseCmpType(), popScConst);
			if (NULL != pdrgprng)
			{
				pcri = GPOS_NEW(mp) CConstraintInterval(mp, colref, pdrgprng, true /*fIncludesNull*/);
			}
		}

		return pcri;
	}
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolOp
//
//	@doc:
//		Create interval from scalar boolean: AND, OR, NOT
//
//---------------------------------------------------------------------------
CConstraintInterval* CConstraintInterval::PciIntervalFromScalarBoolOp(CMemoryPool *mp, CExpression *pexpr, CColRef *colref, BOOL infer_nulls_as)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));

	CScalarBoolOp *popScBool = CScalarBoolOp::PopConvert(pexpr->Pop());
	CScalarBoolOp::EBoolOperator eboolop = popScBool->Eboolop();

	switch (eboolop)
	{
		case CScalarBoolOp::EboolopAnd:
			return PciIntervalFromScalarBoolAnd(mp, pexpr, colref, infer_nulls_as);

		case CScalarBoolOp::EboolopOr:
			return PciIntervalFromScalarBoolOr(mp, pexpr, colref, infer_nulls_as);

		case CScalarBoolOp::EboolopNot:
		{
			CConstraintInterval *pciChild = PciIntervalFromScalarExpr(mp, (*pexpr)[0], colref, !infer_nulls_as);
			if (NULL == pciChild)
			{
				return NULL;
			}

			CConstraintInterval *pciNot = pciChild->PciComplement(mp);
			pciChild->Release();
			return pciNot;
		}
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolOr
//
//	@doc:
//		Create interval from scalar boolean OR
//
//---------------------------------------------------------------------------
CConstraintInterval* CConstraintInterval::PciIntervalFromScalarBoolOr(CMemoryPool *mp, CExpression *pexpr, CColRef *colref, BOOL infer_nulls_as)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));
	GPOS_ASSERT(CScalarBoolOp::EboolopOr == CScalarBoolOp::PopConvert(pexpr->Pop())->Eboolop());

	const ULONG arity = pexpr->Arity();
	GPOS_ASSERT(0 < arity);

	CConstraintIntervalArray *child_constraints = GPOS_NEW(mp) CConstraintIntervalArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CConstraintInterval *pciChild = PciIntervalFromScalarExpr(mp, (*pexpr)[ul], colref, infer_nulls_as);

		if (NULL == pciChild)
		{
			child_constraints->Release();
			return NULL;
		}

		child_constraints->Append(pciChild);
	}

	CConstraintIntervalArray *constraints;

	// PciUnion each interval in pairs. Given intervals I1,I2.., I5, perform the unions as follows:
	// iteration 1: I1 U I2, I3 U I4, I5
	// iteration 2: I12 U I34, I5
	// iteration 3: I1234 U I5
	while (child_constraints->Size() > 1)
	{
		constraints = GPOS_NEW(mp) CConstraintIntervalArray(mp);

		ULONG length = child_constraints->Size();
		ULONG ul;

		for (ul = 0; ul < length - 1; ul += 2)
		{
			CConstraintInterval *pci1 = (*child_constraints)[ul];
			CConstraintInterval *pci2 = (*child_constraints)[ul + 1];

			CConstraintInterval *pciOr = pci1->PciUnion(mp, pci2);
			constraints->Append(pciOr);
		}

		if (ul < length)
		{
			// append the odd one at the end
			CConstraintInterval *pciChild = (*child_constraints)[ul];
			pciChild->AddRef();
			constraints->Append(pciChild);
		}
		child_constraints->Release();
		child_constraints = constraints;
	}
	GPOS_ASSERT(child_constraints->Size() == 1);
	CConstraintInterval *dest = (*child_constraints)[0];
	dest->AddRef();
	child_constraints->Release();

	return dest;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolAnd
//
//	@doc:
//		Create interval from scalar boolean AND
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromScalarBoolAnd(CMemoryPool *mp,
												  CExpression *pexpr,
												  CColRef *colref,
												  BOOL infer_nulls_as)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));
	GPOS_ASSERT(CScalarBoolOp::EboolopAnd ==
				CScalarBoolOp::PopConvert(pexpr->Pop())->Eboolop());

	const ULONG arity = pexpr->Arity();
	GPOS_ASSERT(0 < arity);

	CConstraintInterval *pci =
		PciIntervalFromScalarExpr(mp, (*pexpr)[0], colref, infer_nulls_as);
	for (ULONG ul = 1; ul < arity; ul++)
	{
		CConstraintInterval *pciChild =
			PciIntervalFromScalarExpr(mp, (*pexpr)[ul], colref, infer_nulls_as);
		// here is where we will return a NULL child from not being able to create a
		// CConstraint interval from the ScalarExpr
		if (NULL != pciChild && NULL != pci)
		{
			CConstraintInterval *pciAnd = pci->PciIntersect(mp, pciChild);
			pci->Release();
			pciChild->Release();
			pci = pciAnd;
		}
		else if (NULL != pciChild)
		{
			pci = pciChild;
		}
	}

	return pci;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprScalar
//
//	@doc:
//		Return scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprScalar(CMemoryPool *mp)
{
	if (NULL == m_pexprScalar)
	{
		m_pexprScalar = PexprConstructScalar(mp);
	}

	return m_pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructScalar
//
//	@doc:
//		Construct scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprConstructScalar(CMemoryPool *mp) const
{
	if (FContradiction())
	{
		return CUtils::PexprScalarConstBool(mp, false /*fval*/,
											false /*is_null*/);
	}

	if (GPOS_FTRACE(EopttraceArrayConstraints))
	{
		// try creating an array IN/NOT IN expression
		CExpression *pexpr = PexprConstructArrayScalar(mp);
		if (pexpr != NULL)
		{
			return pexpr;
		}
	}

	// otherwise, we generate a disjunction of ranges
	return PexprConstructDisjunctionScalar(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructDisjunctionScalar
//
//	@doc:
//		Returns a disjunction of several equality or inequality expressions
//		describing this interval. Or, returns a singular expression if the
//		interval can be represented as such.
//		For example an interval containing ranges like
//			[1,1],(7,inf)
//		converts to an expression like
//			x = 1 OR x > 7
//		but an interval containing the range
//			(-inf, inf)
//		converts to a scalar true
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprConstructDisjunctionScalar(CMemoryPool *mp) const
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG length = m_pdrgprng->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CRange *prange = (*m_pdrgprng)[ul];
		CExpression *pexprChild = prange->PexprScalar(mp, m_pcr);
		pdrgpexpr->Append(pexprChild);
	}

	if (1 == pdrgpexpr->Size() && CUtils::FScalarConstTrue((*pdrgpexpr)[0]))
	{
		// so far, interval covers all the not null values
		pdrgpexpr->Release();

		if (m_fIncludesNull)
		{
			return CUtils::PexprScalarConstBool(mp, true /*fval*/,
												false /*is_null*/);
		}

		return CUtils::PexprIsNotNull(mp, CUtils::PexprScalarIdent(mp, m_pcr));
	}

	if (m_fIncludesNull)
	{
		CExpression *pexprIsNull =
			CUtils::PexprIsNull(mp, CUtils::PexprScalarIdent(mp, m_pcr));
		pdrgpexpr->Append(pexprIsNull);
	}

	return CPredicateUtils::PexprDisjunction(mp, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FConvertsToIn
//
//	@doc:
//		Looks for a specific pattern within the array of ranges to determine
//		if this interval can be converted into an array IN statement. The
//		pattern is like [[n,n], [m,m]] is an IN
//
//---------------------------------------------------------------------------
bool
CConstraintInterval::FConvertsToIn() const
{
	if (1 >= m_pdrgprng->Size())
	{
		return false;
	}

	bool isIN = true;
	const ULONG length = m_pdrgprng->Size();
	for (ULONG ul = 0; ul < length && isIN; ul++)
	{
		isIN &= (*m_pdrgprng)[ul]->FPoint();
	}
	return isIN;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FConvertsToNotIn
//
//	@doc:
//		Looks for a specific pattern within the array of ranges to determine
//		if this interval can be converted into an array NOT IN statement. The
//		pattern is like [(-inf, m), (m, n), (n, inf)]
//
//---------------------------------------------------------------------------
bool
CConstraintInterval::FConvertsToNotIn() const
{
	if (1 >= m_pdrgprng->Size())
	{
		return false;
	}

	// for this to be a NOT IN, its edges must be unbounded
	if ((*m_pdrgprng)[0]->PdatumLeft() != NULL ||
		(*m_pdrgprng)[m_pdrgprng->Size() - 1]->PdatumRight() != NULL)
	{
		return false;
	}

	// check that each range is exclusive and that the inner values are equal
	bool isNotIn = true;
	CRange *pLeftRng = (*m_pdrgprng)[0];
	CRange *pRightRng = NULL;
	const ULONG length = m_pdrgprng->Size();
	for (ULONG ul = 1; ul < length && isNotIn; ul++)
	{
		pRightRng = (*m_pdrgprng)[ul];
		isNotIn &= pLeftRng->EriRight() == CRange::EriExcluded;
		isNotIn &= pRightRng->EriLeft() == CRange::EriExcluded;
		isNotIn &= pLeftRng->FUpperBoundEqualsLowerBound(pRightRng);

		pLeftRng = pRightRng;
	}

	return isNotIn;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructArrayScalar
//
//	@doc:
//		Constructs an array expression from the ranges stored in this interval.
//		It is a mistake to call this method without first detecting if the
//		stored ranges can be converted to an IN or NOT in statement. The param
//		'fIn' refers to the statement being an IN statement, and if set to false,
// 		it is considered a NOT IN statement
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprConstructArrayScalar(CMemoryPool *mp, bool fIn) const
{
	GPOS_ASSERT(FConvertsToIn() || FConvertsToNotIn());

	ULONG ulRngs = m_pdrgprng->Size();
	IMDType::ECmpType ecmptype = IMDType::EcmptEq;
	CScalarArrayCmp::EArrCmpType earraycmptype = CScalarArrayCmp::EarrcmpAny;

	if (!fIn)
	{
		ecmptype = IMDType::EcmptNEq;
		earraycmptype = CScalarArrayCmp::EarrcmpAll;

		// if NOT IN, we skip the last range, as the right datum will be null
		ulRngs -= 1;
	}

	// loop through all of the constants in the ranges, creating an array of CScalarConst Expressions
	CExpressionArray *prngexpr = GPOS_NEW(mp) CExpressionArray(mp);

	// this method assumes IN or NOT IN which means that the ranges stored will look like either
	// [x,x], ... ,[y,y] or the NOT IN case (-inf, x),(x,y), ... ,(z,inf).
	for (ULONG ul = 0; ul < ulRngs; ul++)
	{
		IDatum *datum = (*m_pdrgprng)[ul]->PdatumRight();
		datum->AddRef();
		CScalarConst *popScConst = GPOS_NEW(mp) CScalarConst(mp, datum);
		CExpression *pexpr = GPOS_NEW(mp) CExpression(mp, popScConst);
		prngexpr->Append(pexpr);
	}

	CExpression *pexpr = CUtils::PexprScalarArrayCmp(mp, earraycmptype,
													 ecmptype, prngexpr, m_pcr);

	if (m_fIncludesNull)
	{
		CExpression *pexprIsNull =
			CUtils::PexprIsNull(mp, CUtils::PexprScalarIdent(mp, m_pcr));
		CExpression *pexprDisjuction =
			CPredicateUtils::PexprDisjunction(mp, pexpr, pexprIsNull);
		pexpr->Release();
		pexprIsNull->Release();
		pexpr = pexprDisjuction;
	}

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructScalar
//
//	@doc:
//		Constructs an array expression if the interval can be converted into
//		an array expression. Returns null if an array scalar cannot be
//		constructed
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprConstructArrayScalar(CMemoryPool *mp) const
{
	if (1 >= m_pdrgprng->Size())
	{
		return NULL;
	}

	if (FConvertsToIn())
	{
		return PexprConstructArrayScalar(mp, true);
	}
	else if (FConvertsToNotIn())
	{
		return PexprConstructArrayScalar(mp, false);
	}
	else
	{
		// Does not convert to either IN or NOT IN
		return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::Pcnstr
//
//	@doc:
//		Return constraint on a given column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintInterval::Pcnstr(CMemoryPool *,	//mp,
							const CColRef *colref)
{
	if (m_pcr == colref)
	{
		this->AddRef();
		return this;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::Pcnstr
//
//	@doc:
//		Return constraint on a given column set
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintInterval::Pcnstr(CMemoryPool *,	//mp,
							CColRefSet *pcrs)
{
	if (pcrs->FMember(m_pcr))
	{
		this->AddRef();
		return this;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PcnstrRemapForColumn
//
//	@doc:
//		Return a copy of the constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintInterval::PcnstrRemapForColumn(CMemoryPool *mp,
										  CColRef *colref) const
{
	GPOS_ASSERT(NULL != colref);
	m_pdrgprng->AddRef();
	return GPOS_NEW(mp)
		CConstraintInterval(mp, colref, m_pdrgprng, m_fIncludesNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntersect
//
//	@doc:
//		Intersection with another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntersect(CMemoryPool *mp, CConstraintInterval *pci)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(m_pcr == pci->Pcr());

	CRangeArray *pdrgprngOther = pci->Pdrgprng();

	CRangeArray *pdrgprngNew = GPOS_NEW(mp) CRangeArray(mp);

	ULONG ulFst = 0;
	ULONG ulSnd = 0;
	const ULONG ulNumRangesFst = m_pdrgprng->Size();
	const ULONG ulNumRangesSnd = pdrgprngOther->Size();
	while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd)
	{
		CRange *prangeThis = (*m_pdrgprng)[ulFst];
		CRange *prangeOther = (*pdrgprngOther)[ulSnd];

		CRange *prangeNew = NULL;
		if (prangeOther->FEndsAfter(prangeThis))
		{
			prangeNew = prangeThis->PrngIntersect(mp, prangeOther);
			ulFst++;
		}
		else
		{
			prangeNew = prangeOther->PrngIntersect(mp, prangeThis);
			ulSnd++;
		}

		if (NULL != prangeNew)
		{
			pdrgprngNew->Append(prangeNew);
		}
	}

	return GPOS_NEW(mp) CConstraintInterval(
		mp, m_pcr, pdrgprngNew, m_fIncludesNull && pci->FIncludesNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnion
//
//	@doc:
//		Union with another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciUnion(CMemoryPool *mp, CConstraintInterval *pci)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(m_pcr == pci->Pcr());

	CRangeArray *pdrgprngOther = pci->Pdrgprng();

	CRangeArray *pdrgprngNew = GPOS_NEW(mp) CRangeArray(mp);

	ULONG ulFst = 0;
	ULONG ulSnd = 0;
	const ULONG ulNumRangesFst = m_pdrgprng->Size();
	const ULONG ulNumRangesSnd = pdrgprngOther->Size();
	while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd)
	{
		CRange *prangeThis = (*m_pdrgprng)[ulFst];
		CRange *prangeOther = (*pdrgprngOther)[ulSnd];

		CRange *prangeNew = NULL;
		if (prangeOther->FEndsAfter(prangeThis))
		{
			prangeNew = prangeThis->PrngDifferenceLeft(mp, prangeOther);
			ulFst++;
		}
		else
		{
			prangeNew = prangeOther->PrngDifferenceLeft(mp, prangeThis);
			ulSnd++;
		}

		AppendOrExtend(mp, pdrgprngNew, prangeNew);
	}

	AddRemainingRanges(mp, m_pdrgprng, ulFst, pdrgprngNew);
	AddRemainingRanges(mp, pdrgprngOther, ulSnd, pdrgprngNew);

	return GPOS_NEW(mp) CConstraintInterval(
		mp, m_pcr, pdrgprngNew, m_fIncludesNull || pci->FIncludesNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciDifference
//
//	@doc:
//		Difference between this interval and another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciDifference(CMemoryPool *mp, CConstraintInterval *pci)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(m_pcr == pci->Pcr());

	CRangeArray *pdrgprngOther = pci->Pdrgprng();

	CRangeArray *pdrgprngNew = GPOS_NEW(mp) CRangeArray(mp);

	ULONG ulFst = 0;
	ULONG ulSnd = 0;
	CRangeArray *pdrgprngResidual = GPOS_NEW(mp) CRangeArray(mp);
	CRange *prangeResidual = NULL;
	const ULONG ulNumRangesFst = m_pdrgprng->Size();
	const ULONG ulNumRangesSnd = pdrgprngOther->Size();
	while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd)
	{
		// if there is a residual range from previous iteration then use it
		CRange *prangeThis =
			(NULL == prangeResidual ? (*m_pdrgprng)[ulFst] : prangeResidual);
		CRange *prangeOther = (*pdrgprngOther)[ulSnd];

		CRange *prangeNew = NULL;
		prangeResidual = NULL;

		if (prangeOther->FEndsWithOrAfter(prangeThis))
		{
			prangeNew = prangeThis->PrngDifferenceLeft(mp, prangeOther);
			ulFst++;
		}
		else
		{
			prangeNew = PrangeDiffWithRightResidual(
				mp, prangeThis, prangeOther, &prangeResidual, pdrgprngResidual);
			ulSnd++;
		}

		AppendOrExtend(mp, pdrgprngNew, prangeNew);
	}

	if (NULL != prangeResidual)
	{
		ulFst++;
		prangeResidual->AddRef();
	}

	AppendOrExtend(mp, pdrgprngNew, prangeResidual);
	pdrgprngResidual->Release();
	AddRemainingRanges(mp, m_pdrgprng, ulFst, pdrgprngNew);

	return GPOS_NEW(mp) CConstraintInterval(
		mp, m_pcr, pdrgprngNew, m_fIncludesNull && !pci->FIncludesNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FContainsInterval
//
//	@doc:
//		Does the current interval contain the given interval?
//
//---------------------------------------------------------------------------
BOOL
CConstraintInterval::FContainsInterval(CMemoryPool *mp,
									   CConstraintInterval *pci)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(m_pcr == pci->Pcr());

	if (IsConstraintUnbounded())
	{
		return true;
	}

	if (NULL == pci || pci->IsConstraintUnbounded() ||
		(!FIncludesNull() && pci->FIncludesNull()))
	{
		return false;
	}

	CConstraintInterval *pciDiff = pci->PciDifference(mp, this);

	// if the difference is empty, then this interval contains the given one
	BOOL fContains = pciDiff->FContradiction();
	pciDiff->Release();

	return fContains;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnbounded
//
//	@doc:
//		Create an unbounded interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciUnbounded(CMemoryPool *mp, const CColRef *colref,
								  BOOL fIncludesNull)
{
	IMDId *mdid = colref->RetrieveType()->MDId();
	if (!CUtils::FConstrainableType(mdid))
	{
		return NULL;
	}

	mdid->AddRef();
	CRange *prange = GPOS_NEW(mp)
		CRange(mdid, COptCtxt::PoctxtFromTLS()->Pcomp(), NULL /*ppointLeft*/,
			   CRange::EriExcluded, NULL /*ppointRight*/, CRange::EriExcluded);

	CRangeArray *pdrgprng = GPOS_NEW(mp) CRangeArray(mp);
	pdrgprng->Append(prange);

	return GPOS_NEW(mp)
		CConstraintInterval(mp, colref, pdrgprng, fIncludesNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnbounded
//
//	@doc:
//		Create an unbounded interval on any column from the given set
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciUnbounded(CMemoryPool *mp, const CColRefSet *pcrs,
								  BOOL fIncludesNull)
{
	// find the first constrainable column
	CColRefSetIter crsi(*pcrs);
	while (crsi.Advance())
	{
		CColRef *colref = crsi.Pcr();
		CConstraintInterval *pci = PciUnbounded(mp, colref, fIncludesNull);
		if (NULL != pci)
		{
			return pci;
		}
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::MdidType
//
//	@doc:
//		Type of this interval
//
//---------------------------------------------------------------------------
IMDId *
CConstraintInterval::MdidType()
{
	// if there is at least one range, return range type
	if (0 < m_pdrgprng->Size())
	{
		CRange *prange = (*m_pdrgprng)[0];
		return prange->MDId();
	}

	// otherwise return type of column ref
	return m_pcr->RetrieveType()->MDId();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciComplement
//
//	@doc:
//		Complement of this interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciComplement(CMemoryPool *mp)
{
	// create an unbounded interval
	CConstraintInterval *pciUniversal =
		PciUnbounded(mp, m_pcr, true /*fIncludesNull*/);

	CConstraintInterval *pciComp = pciUniversal->PciDifference(mp, this);
	pciUniversal->Release();

	return pciComp;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PrangeDiffWithRightResidual
//
//	@doc:
//		Difference between two ranges on the left side only -
//		Any difference on the right side is reported as residual range
//
//		this    |----------------------|
//		prange         |-----------|
//		result  |------|
//		residual                   |---|
//---------------------------------------------------------------------------
CRange *
CConstraintInterval::PrangeDiffWithRightResidual(CMemoryPool *mp,
												 CRange *prangeFirst,
												 CRange *prangeSecond,
												 CRange **pprangeResidual,
												 CRangeArray *pdrgprngResidual)
{
	if (prangeSecond->FDisjointLeft(prangeFirst))
	{
		return NULL;
	}

	CRange *prangeRet = NULL;

	if (prangeFirst->Contains(prangeSecond))
	{
		prangeRet = prangeFirst->PrngDifferenceLeft(mp, prangeSecond);
	}

	// the part of prangeFirst that goes beyond prangeSecond
	*pprangeResidual = prangeFirst->PrngDifferenceRight(mp, prangeSecond);
	// add it to array so we can release it later on
	if (NULL != *pprangeResidual)
	{
		pdrgprngResidual->Append(*pprangeResidual);
	}

	return prangeRet;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::AddRemainingRanges
//
//	@doc:
//		Add ranges from a source array to a destination array, starting at the
//		range with the given index
//
//---------------------------------------------------------------------------
void CConstraintInterval::AddRemainingRanges(CMemoryPool *mp, CRangeArray *pdrgprngSrc, ULONG ulStart, CRangeArray *pdrgprngDest)
{
	const ULONG length = pdrgprngSrc->Size();
	for (ULONG ul = ulStart; ul < length; ul++)
	{
		CRange *prange = (*pdrgprngSrc)[ul];
		prange->AddRef();
		AppendOrExtend(mp, pdrgprngDest, prange);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::AppendOrExtend
//
//	@doc:
//		Append the given range to the array or extend the last range in that
//		array
//
//---------------------------------------------------------------------------
void CConstraintInterval::AppendOrExtend(CMemoryPool *mp, CRangeArray *pdrgprng, CRange *prange)
{
	if (NULL == prange)
	{
		return;
	}

	GPOS_ASSERT(NULL != pdrgprng);

	const ULONG length = pdrgprng->Size();
	if (0 == length)
	{
		pdrgprng->Append(prange);
		return;
	}

	CRange *prangeLast = (*pdrgprng)[length - 1];
	CRange *prangeNew = prangeLast->PrngExtend(mp, prange);
	if (NULL == prangeNew)
	{
		pdrgprng->Append(prange);
	}
	else
	{
		pdrgprng->Replace(length - 1, prangeNew);
		prange->Release();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::OsPrint
//
//	@doc:
//		Debug print interval
//
//---------------------------------------------------------------------------
IOstream& CConstraintInterval::OsPrint(IOstream &os) const
{
	os << "{";
	m_pcr->OsPrint(os);
	const ULONG length = m_pdrgprng->Size();
	os << ", ranges: ";
	for (ULONG ul = 0; ul < length; ul++)
	{
		CRange *prange = (*m_pdrgprng)[ul];
		os << *prange << " ";
	}

	if (m_fIncludesNull)
	{
		os << "[NULL] ";
	}
	os << "}";
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CRange::PciRangeFromColConstCmp
//
//	@doc:
//		Creates an array of 1 or 2 ranges which represent the comparison to
//		a scalar.
//
//---------------------------------------------------------------------------
CRangeArray* CConstraintInterval::PciRangeFromColConstCmp(CMemoryPool *mp, IMDType::ECmpType cmp_type, const CScalarConst *popsccnst)
{
	GPOS_ASSERT(CScalar::EopScalarConst == popsccnst->Eopid());

	// comparison operator
	if (IMDType::EcmptOther == cmp_type)
	{
		return NULL;
	}

	IDatum *datum = popsccnst->GetDatum();
	CRangeArray *pdrgprng = GPOS_NEW(mp) CRangeArray(mp);

	const IComparator *pcomp = COptCtxt::PoctxtFromTLS()->Pcomp();
	if (IMDType::EcmptNEq == cmp_type || IMDType::EcmptIDF == cmp_type)
	{
		// need an interval with 2 ranges
		datum->AddRef();
		pdrgprng->Append(GPOS_NEW(mp) CRange(pcomp, IMDType::EcmptL, datum));
		datum->AddRef();
		pdrgprng->Append(GPOS_NEW(mp) CRange(pcomp, IMDType::EcmptG, datum));
	}
	else
	{
		datum->AddRef();
		pdrgprng->Append(GPOS_NEW(mp) CRange(pcomp, cmp_type, datum));
	}

	return pdrgprng;
}