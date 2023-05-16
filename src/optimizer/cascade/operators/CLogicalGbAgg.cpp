//---------------------------------------------------------------------------
//	@filename:
//		CLogicalGbAgg.cpp
//
//	@doc:
//		Implementation of aggregate operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalGbAgg.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/statistics/CGroupByStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::CLogicalGbAgg
//
//	@doc:
//		ctor for xform pattern
//
//---------------------------------------------------------------------------
CLogicalGbAgg::CLogicalGbAgg(CMemoryPool *mp)
	: CLogicalUnary(mp), m_fGeneratesDuplicates(true), m_pdrgpcrArgDQA(NULL), m_pdrgpcr(NULL), m_pdrgpcrMinimal(NULL), m_egbaggtype(COperator::EgbaggtypeSentinel), m_aggStage(EasOthers)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::CLogicalGbAgg
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalGbAgg::CLogicalGbAgg(CMemoryPool *mp, CColRefArray *colref_array, COperator::EGbAggType egbaggtype)
	: CLogicalUnary(mp), m_fGeneratesDuplicates(false), m_pdrgpcrArgDQA(NULL), m_pdrgpcr(colref_array), m_pdrgpcrMinimal(NULL), m_egbaggtype(egbaggtype), m_aggStage(EasOthers)
{
	if (COperator::EgbaggtypeLocal == egbaggtype)
	{
		// final and intermediate aggregates have to remove duplicates for a given group
		m_fGeneratesDuplicates = true;
	}
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
	GPOS_ASSERT(COperator::EgbaggtypeIntermediate != egbaggtype);
	m_pcrsLocalUsed->Include(m_pdrgpcr);
}

CLogicalGbAgg::CLogicalGbAgg(CMemoryPool *mp, CColRefArray *colref_array, COperator::EGbAggType egbaggtype, EAggStage aggStage)
	: CLogicalUnary(mp), m_fGeneratesDuplicates(false), m_pdrgpcrArgDQA(NULL), m_pdrgpcr(colref_array), m_pdrgpcrMinimal(NULL), m_egbaggtype(egbaggtype), m_aggStage(aggStage)
{
	if (COperator::EgbaggtypeLocal == egbaggtype)
	{
		// final and intermediate aggregates have to remove duplicates for a given group
		m_fGeneratesDuplicates = true;
	}
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
	GPOS_ASSERT(COperator::EgbaggtypeIntermediate != egbaggtype);
	m_pcrsLocalUsed->Include(m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::CLogicalGbAgg
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalGbAgg::CLogicalGbAgg(CMemoryPool *mp, CColRefArray *colref_array, COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates, CColRefArray *pdrgpcrArgDQA)
	: CLogicalUnary(mp), m_fGeneratesDuplicates(fGeneratesDuplicates), m_pdrgpcrArgDQA(pdrgpcrArgDQA), m_pdrgpcr(colref_array), m_pdrgpcrMinimal(NULL), m_egbaggtype(egbaggtype), m_aggStage(EasOthers)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
	GPOS_ASSERT_IMP(NULL == m_pdrgpcrArgDQA, COperator::EgbaggtypeIntermediate != egbaggtype);
	GPOS_ASSERT_IMP(m_fGeneratesDuplicates, COperator::EgbaggtypeLocal == egbaggtype);
	m_pcrsLocalUsed->Include(m_pdrgpcr);
}

CLogicalGbAgg::CLogicalGbAgg(CMemoryPool *mp, CColRefArray *colref_array, COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates, CColRefArray *pdrgpcrArgDQA, EAggStage aggStage)
	: CLogicalUnary(mp), m_fGeneratesDuplicates(fGeneratesDuplicates), m_pdrgpcrArgDQA(pdrgpcrArgDQA), m_pdrgpcr(colref_array), m_pdrgpcrMinimal(NULL), m_egbaggtype(egbaggtype), m_aggStage(aggStage)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
	GPOS_ASSERT_IMP(NULL == m_pdrgpcrArgDQA, COperator::EgbaggtypeIntermediate != egbaggtype);
	GPOS_ASSERT_IMP(m_fGeneratesDuplicates, COperator::EgbaggtypeLocal == egbaggtype);
	m_pcrsLocalUsed->Include(m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::CLogicalGbAgg
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalGbAgg::CLogicalGbAgg(CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal, COperator::EGbAggType egbaggtype)
	: CLogicalUnary(mp), m_fGeneratesDuplicates(true), m_pdrgpcrArgDQA(NULL), m_pdrgpcr(colref_array), m_pdrgpcrMinimal(pdrgpcrMinimal), m_egbaggtype(egbaggtype), m_aggStage(EasOthers)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
	GPOS_ASSERT(COperator::EgbaggtypeIntermediate != egbaggtype);
	GPOS_ASSERT_IMP(NULL != pdrgpcrMinimal, pdrgpcrMinimal->Size() <= colref_array->Size());
	if (NULL == pdrgpcrMinimal)
	{
		m_pdrgpcr->AddRef();
		m_pdrgpcrMinimal = m_pdrgpcr;
	}
	m_pcrsLocalUsed->Include(m_pdrgpcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::CLogicalGbAgg
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalGbAgg::CLogicalGbAgg(CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal, COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates, CColRefArray *pdrgpcrArgDQA)
	: CLogicalUnary(mp), m_fGeneratesDuplicates(fGeneratesDuplicates), m_pdrgpcrArgDQA(pdrgpcrArgDQA), m_pdrgpcr(colref_array), m_pdrgpcrMinimal(pdrgpcrMinimal), m_egbaggtype(egbaggtype), m_aggStage(EasOthers)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
	GPOS_ASSERT_IMP(NULL != pdrgpcrMinimal, pdrgpcrMinimal->Size() <= colref_array->Size());
	GPOS_ASSERT_IMP(NULL == m_pdrgpcrArgDQA, COperator::EgbaggtypeIntermediate != egbaggtype);
	GPOS_ASSERT_IMP(m_fGeneratesDuplicates, COperator::EgbaggtypeLocal == egbaggtype);

	if (NULL == pdrgpcrMinimal)
	{
		m_pdrgpcr->AddRef();
		m_pdrgpcrMinimal = m_pdrgpcr;
	}

	m_pcrsLocalUsed->Include(m_pdrgpcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::~CLogicalGbAgg
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalGbAgg::~CLogicalGbAgg()
{
	// safe release -- to allow for instances used in patterns
	CRefCount::SafeRelease(m_pdrgpcr);
	CRefCount::SafeRelease(m_pdrgpcrMinimal);
	CRefCount::SafeRelease(m_pdrgpcrArgDQA);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalGbAgg::PopCopyWithRemappedColumns(CMemoryPool *mp,
										  UlongToColRefMap *colref_mapping,
										  BOOL must_exist)
{
	CColRefArray *colref_array =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcr, colref_mapping, must_exist);
	CColRefArray *pdrgpcrMinimal = NULL;
	if (NULL != m_pdrgpcrMinimal)
	{
		pdrgpcrMinimal = CUtils::PdrgpcrRemap(mp, m_pdrgpcrMinimal,
											  colref_mapping, must_exist);
	}

	CColRefArray *pdrgpcrArgDQA = NULL;
	if (NULL != m_pdrgpcrArgDQA)
	{
		pdrgpcrArgDQA = CUtils::PdrgpcrRemap(mp, m_pdrgpcrArgDQA,
											 colref_mapping, must_exist);
	}

	return GPOS_NEW(mp)
		CLogicalGbAgg(mp, colref_array, pdrgpcrMinimal, Egbaggtype(),
					  m_fGeneratesDuplicates, pdrgpcrArgDQA);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGbAgg::DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	// include the intersection of the grouping columns and the child's output
	pcrs->Include(Pdrgpcr());
	pcrs->Intersection(exprhdl.DeriveOutputColumns(0));

	// the scalar child defines additional columns
	pcrs->Union(exprhdl.DeriveDefinedColumns(1));

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::DeriveOuterReferences
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGbAgg::DeriveOuterReferences(CMemoryPool *mp,
									 CExpressionHandle &exprhdl)
{
	CColRefSet *pcrsGrp = GPOS_NEW(mp) CColRefSet(mp);
	pcrsGrp->Include(m_pdrgpcr);

	CColRefSet *outer_refs =
		CLogical::DeriveOuterReferences(mp, exprhdl, pcrsGrp);
	pcrsGrp->Release();

	return outer_refs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalGbAgg::DerivePropertyConstraint(CMemoryPool *mp,
										CExpressionHandle &exprhdl) const
{
	CColRefSet *pcrsGrouping = GPOS_NEW(mp) CColRefSet(mp);
	pcrsGrouping->Include(m_pdrgpcr);

	// get the constraints on the grouping columns only
	CPropConstraint *ppc =
		PpcDeriveConstraintRestrict(mp, exprhdl, pcrsGrouping);
	pcrsGrouping->Release();

	return ppc;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::PcrsStat
//
//	@doc:
//		Compute required stats columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGbAgg::PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
						CColRefSet *pcrsInput, ULONG child_index) const
{
	return PcrsStatGbAgg(mp, exprhdl, pcrsInput, child_index, m_pdrgpcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::PcrsStatGbAgg
//
//	@doc:
//		Compute required stats columns for a GbAgg
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGbAgg::PcrsStatGbAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 CColRefSet *pcrsInput, ULONG child_index,
							 CColRefArray *pdrgpcrGrp) const
{
	GPOS_ASSERT(NULL != pdrgpcrGrp);
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	// include grouping columns
	pcrs->Include(pdrgpcrGrp);

	// other columns used in aggregates
	pcrs->Union(exprhdl.DeriveUsedColumns(1));

	// if the grouping column is a computed column, then add its corresponding used columns
	// to required columns for statistics computation
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	const ULONG ulGrpCols = m_pdrgpcr->Size();
	for (ULONG ul = 0; ul < ulGrpCols; ul++)
	{
		CColRef *pcrGrpCol = (*m_pdrgpcr)[ul];
		const CColRefSet *pcrsUsed =
			col_factory->PcrsUsedInComputedCol(pcrGrpCol);
		if (NULL != pcrsUsed)
		{
			pcrs->Union(pcrsUsed);
		}
	}

	CColRefSet *pcrsRequired =
		PcrsReqdChildStats(mp, exprhdl, pcrsInput, pcrs, child_index);
	pcrs->Release();

	return pcrsRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::DeriveNotNullColumns
//
//	@doc:
//		Derive not null columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGbAgg::DeriveNotNullColumns(CMemoryPool *mp,
									CExpressionHandle &exprhdl) const
{
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	// include grouping columns
	pcrs->Include(Pdrgpcr());

	// intersect with not nullable columns from relational child
	pcrs->Intersection(exprhdl.DeriveNotNullColumns(0));

	// TODO,  03/18/2012, add nullability info of computed columns

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalGbAgg::HashValue() const
{
	ULONG ulHash = COperator::HashValue();
	ULONG arity = m_pdrgpcr->Size();
	ULONG ulGbaggtype = (ULONG) m_egbaggtype;

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CColRef *colref = (*m_pdrgpcr)[ul];
		ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(colref));
	}

	ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<ULONG>(&ulGbaggtype));

	return gpos::CombineHashes(ulHash,
							   gpos::HashValue<BOOL>(&m_fGeneratesDuplicates));
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalGbAgg::DeriveKeyCollection(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const
{
	CKeyCollection *pkc = NULL;

	// Gb produces a key only if it's global
	if (FGlobal())
	{
		if (COperator::EgbaggtypeLocal == m_egbaggtype &&
			!m_fGeneratesDuplicates)
		{
			return pkc;
		}

		if (0 < m_pdrgpcr->Size())
		{
			// grouping columns always constitute a key
			m_pdrgpcr->AddRef();
			pkc = GPOS_NEW(mp) CKeyCollection(mp, m_pdrgpcr);
		}
		else
		{
			// scalar and single-group aggs produce one row that constitutes a key
			CColRefSet *pcrs = exprhdl.DeriveDefinedColumns(1);

			if (0 == pcrs->Size())
			{
				// aggregate defines no columns, e.g. select 1 from r group by a
				return NULL;
			}

			pcrs->AddRef();
			pkc = GPOS_NEW(mp) CKeyCollection(mp, pcrs);
		}
	}

	return pkc;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalGbAgg::DeriveMaxCard(CMemoryPool *,	 //mp
							 CExpressionHandle &exprhdl) const
{
	// agg w/o grouping columns produces one row
	if (0 == m_pdrgpcr->Size())
	{
		return CMaxCard(1 /*ull*/);
	}

	// contradictions produce no rows
	if (exprhdl.DerivePropertyConstraint()->FContradiction())
	{
		return CMaxCard(0 /*ull*/);
	}

	return CMaxCard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalGbAgg::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalGbAgg *popAgg = reinterpret_cast<CLogicalGbAgg *>(pop);

	return FGeneratesDuplicates() == popAgg->FGeneratesDuplicates() &&
		   popAgg->Egbaggtype() == m_egbaggtype &&
		   CColRef::Equals(m_pdrgpcr, popAgg->m_pdrgpcr) &&
		   CColRef::Equals(m_pdrgpcrMinimal, popAgg->PdrgpcrMinimal()) &&
		   CColRef::Equals(m_pdrgpcrArgDQA, popAgg->PdrgpcrArgDQA());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalGbAgg::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfSimplifyGbAgg);
	(void) xform_set->ExchangeSet(CXform::ExfGbAggWithMDQA2Join);
	(void) xform_set->ExchangeSet(CXform::ExfCollapseGbAgg);
	(void) xform_set->ExchangeSet(CXform::ExfPushGbBelowJoin);
	(void) xform_set->ExchangeSet(CXform::ExfPushGbBelowUnion);
	(void) xform_set->ExchangeSet(CXform::ExfPushGbBelowUnionAll);
	(void) xform_set->ExchangeSet(CXform::ExfSplitGbAgg);
	(void) xform_set->ExchangeSet(CXform::ExfSplitDQA);
	(void) xform_set->ExchangeSet(CXform::ExfGbAgg2Apply);
	(void) xform_set->ExchangeSet(CXform::ExfGbAgg2HashAgg);
	(void) xform_set->ExchangeSet(CXform::ExfGbAgg2StreamAgg);
	(void) xform_set->ExchangeSet(CXform::ExfGbAgg2ScalarAgg);
	(void) xform_set->ExchangeSet(CXform::ExfEagerAgg);
	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalGbAgg::PstatsDerive(CMemoryPool *mp, IStatistics *child_stats,
							CColRefArray *pdrgpcrGroupingCols,
							ULongPtrArray *pdrgpulComputedCols, CBitSet *keys)
{
	const ULONG ulGroupingCols = pdrgpcrGroupingCols->Size();

	// extract grouping column ids
	ULongPtrArray *pdrgpulGroupingCols = GPOS_NEW(mp) ULongPtrArray(mp);
	for (ULONG ul = 0; ul < ulGroupingCols; ul++)
	{
		CColRef *colref = (*pdrgpcrGroupingCols)[ul];
		pdrgpulGroupingCols->Append(GPOS_NEW(mp) ULONG(colref->Id()));
	}

	IStatistics *stats = CGroupByStatsProcessor::CalcGroupByStats(
		mp, dynamic_cast<CStatistics *>(child_stats), pdrgpulGroupingCols,
		pdrgpulComputedCols, keys);

	// clean up
	pdrgpulGroupingCols->Release();

	return stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalGbAgg::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							IStatisticsArray *	// not used
) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *child_stats = exprhdl.Pstats(0);

	// extract computed columns
	ULongPtrArray *pdrgpulComputedCols = GPOS_NEW(mp) ULongPtrArray(mp);
	exprhdl.DeriveDefinedColumns(1)->ExtractColIds(mp, pdrgpulComputedCols);

	IStatistics *stats = PstatsDerive(mp, child_stats, Pdrgpcr(),
									  pdrgpulComputedCols, NULL /*keys*/);

	pdrgpulComputedCols->Release();

	return stats;
}

BOOL
CLogicalGbAgg::IsTwoStageScalarDQA() const
{
	return (m_aggStage == EasTwoStageScalarDQA);
}

BOOL
CLogicalGbAgg::IsThreeStageScalarDQA() const
{
	return (m_aggStage == EasThreeStageScalarDQA);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalGbAgg::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << "( ";
	OsPrintGbAggType(os, m_egbaggtype);
	os << " )";
	os << " Grp Cols: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]"
	   << "[";
	OsPrintGbAggType(os, m_egbaggtype);
	os << "]";

	os << ", Minimal Grp Cols: [";
	if (NULL != m_pdrgpcrMinimal)
	{
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrMinimal);
	}
	os << "]";

	if (COperator::EgbaggtypeIntermediate == m_egbaggtype)
	{
		os << ", Distinct Cols:[";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrArgDQA);
		os << "]";
	}

	os << ", Generates Duplicates :[ " << FGeneratesDuplicates() << " ] ";

	if (IsTwoStageScalarDQA())
	{
		os << ", m_aggStage :[  Two Stage Scalar DQA  ] ";
	}

	if (IsThreeStageScalarDQA())
	{
		os << ", m_aggStage :[  Three Stage Scalar DQA  ] ";
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::OsPrintGbAggType
//
//	@doc:
//		Helper function to print aggregate type
//
//---------------------------------------------------------------------------
IOstream &
CLogicalGbAgg::OsPrintGbAggType(IOstream &os, COperator::EGbAggType egbaggtype)
{
	switch (egbaggtype)
	{
		case COperator::EgbaggtypeGlobal:
			os << "Global";
			break;

		case COperator::EgbaggtypeIntermediate:
			os << "Intermediate";
			break;

		case COperator::EgbaggtypeLocal:
			os << "Local";
			break;

		default:
			GPOS_ASSERT(!"Unsupported aggregate type");
	}
	return os;
}