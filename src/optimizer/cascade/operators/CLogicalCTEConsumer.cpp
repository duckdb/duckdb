//---------------------------------------------------------------------------
//	@filename:
//		CLogicalCTEConsumer.cpp
//
//	@doc:
//		Implementation of CTE consumer operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalCTEConsumer.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CMaxCard.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogicalCTEProducer.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CLogicalCTEConsumer
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::CLogicalCTEConsumer(CMemoryPool *mp)
	: CLogical(mp), m_id(0), m_pdrgpcr(NULL), m_pexprInlined(NULL), m_phmulcr(NULL), m_pcrsOutput(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CLogicalCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::CLogicalCTEConsumer(CMemoryPool *mp, ULONG id, CColRefArray *colref_array)
	: CLogical(mp), m_id(id), m_pdrgpcr(colref_array), m_pexprInlined(NULL), m_phmulcr(NULL)
{
	GPOS_ASSERT(NULL != colref_array);
	m_pcrsOutput = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcr);
	CreateInlinedExpr(mp);
	m_pcrsLocalUsed->Include(m_pdrgpcr);
	// map consumer columns to their positions in consumer output
	COptCtxt::PoctxtFromTLS()->Pcteinfo()->AddConsumerCols(id, m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::~CLogicalCTEConsumer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalCTEConsumer::~CLogicalCTEConsumer()
{
	CRefCount::SafeRelease(m_pdrgpcr);
	CRefCount::SafeRelease(m_pexprInlined);
	CRefCount::SafeRelease(m_phmulcr);
	CRefCount::SafeRelease(m_pcrsOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::CreateInlinedExpr
//
//	@doc:
//		Create the inlined version of this consumer as well as the column mapping
//
//---------------------------------------------------------------------------
void CLogicalCTEConsumer::CreateInlinedExpr(CMemoryPool *mp)
{
	CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexprProducer);
	// the actual definition of the CTE is the first child of the producer
	CExpression *pexprCTEDef = (*pexprProducer)[0];

	CLogicalCTEProducer *popProducer = CLogicalCTEProducer::PopConvert(pexprProducer->Pop());

	m_phmulcr = CUtils::PhmulcrMapping(mp, popProducer->Pdrgpcr(), m_pdrgpcr);
	m_pexprInlined = pexprCTEDef->PexprCopyWithRemappedColumns(mp, m_phmulcr, true);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEConsumer::DeriveOutputColumns(CMemoryPool* mp, CExpressionHandle &exprhdl)
{
	m_pcrsOutput->AddRef();
	return m_pcrsOutput;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DeriveNotNullColumns
//
//	@doc:
//		Derive not nullable output columns
//
//---------------------------------------------------------------------------
CColRefSet* CLogicalCTEConsumer::DeriveNotNullColumns(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexprProducer);

	// find producer's not null columns
	CColRefSet *pcrsProducerNotNull = pexprProducer->DeriveNotNullColumns();

	// map producer's not null columns to consumer's output columns
	CColRefSet *pcrsConsumerNotNull = CUtils::PcrsRemap(
		mp, pcrsProducerNotNull, m_phmulcr, true /*must_exist*/);
	GPOS_ASSERT(pcrsConsumerNotNull->Size() == pcrsProducerNotNull->Size());

	return pcrsConsumerNotNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalCTEConsumer::DeriveKeyCollection(CMemoryPool *,		  //mp,
										 CExpressionHandle &  //exprhdl
) const
{
	CExpression *pexpr =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexpr);
	CKeyCollection *pkc = pexpr->DeriveKeyCollection();
	if (NULL != pkc)
	{
		pkc->AddRef();
	}

	return pkc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DerivePartitionInfo
//
//	@doc:
//		Derive partition consumers
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalCTEConsumer::DerivePartitionInfo(CMemoryPool *,		  //mp,
										 CExpressionHandle &  //exprhdl
) const
{
	CPartInfo *ppartInfo = m_pexprInlined->DerivePartitionInfo();
	ppartInfo->AddRef();

	return ppartInfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalCTEConsumer::DeriveMaxCard(CMemoryPool *,		//mp,
								   CExpressionHandle &	//exprhdl
) const
{
	CExpression *pexpr =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexpr);
	return pexpr->DeriveMaxCard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DeriveJoinDepth
//
//	@doc:
//		Derive join depth
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEConsumer::DeriveJoinDepth(CMemoryPool *,		  //mp,
									 CExpressionHandle &  //exprhdl
) const
{
	CExpression *pexpr =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexpr);
	return pexpr->DeriveJoinDepth();
}

// derive table descriptor
CTableDescriptor *
CLogicalCTEConsumer::DeriveTableDescriptor(CMemoryPool *,		//mp
										   CExpressionHandle &	//exprhdl
) const
{
	CExpression *pexpr =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexpr);
	return pexpr->DeriveTableDescriptor();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEConsumer::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalCTEConsumer *popCTEConsumer = CLogicalCTEConsumer::PopConvert(pop);

	return m_id == popCTEConsumer->UlCTEId() &&
		   m_pdrgpcr->Equals(popCTEConsumer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEConsumer::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEConsumer::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalCTEConsumer::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRefArray *colref_array = NULL;
	if (must_exist)
	{
		colref_array =
			CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcr, colref_mapping);
	}
	else
	{
		colref_array =
			CUtils::PdrgpcrRemap(mp, m_pdrgpcr, colref_mapping, must_exist);
	}
	return GPOS_NEW(mp) CLogicalCTEConsumer(mp, m_id, colref_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalCTEConsumer::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfInlineCTEConsumer);
	(void) xform_set->ExchangeSet(CXform::ExfImplementCTEConsumer);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalCTEConsumer::DerivePropertyConstraint(CMemoryPool *mp,
											  CExpressionHandle &  //exprhdl
) const
{
	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexprProducer);
	CPropConstraint *ppc = pexprProducer->DerivePropertyConstraint();
	CColRefSetArray *pdrgpcrs = ppc->PdrgpcrsEquivClasses();
	CConstraint *pcnstr = ppc->Pcnstr();

	// remap producer columns to consumer columns
	CColRefSetArray *pdrgpcrsMapped = GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG length = pdrgpcrs->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRefSet *pcrs = (*pdrgpcrs)[ul];
		CColRefSet *pcrsMapped =
			CUtils::PcrsRemap(mp, pcrs, m_phmulcr, true /*must_exist*/);
		pdrgpcrsMapped->Append(pcrsMapped);
	}

	CConstraint *pcnstrMapped = NULL;
	if (NULL != pcnstr)
	{
		pcnstrMapped = pcnstr->PcnstrCopyWithRemappedColumns(
			mp, m_phmulcr, true /*must_exist*/);
	}

	return GPOS_NEW(mp) CPropConstraint(mp, pdrgpcrsMapped, pcnstrMapped);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::PstatsDerive
//
//	@doc:
//		Derive statistics based on cte producer
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalCTEConsumer::PstatsDerive(CMemoryPool *mp,
								  CExpressionHandle &,	//exprhdl,
								  IStatisticsArray *	// statistics_array
) const
{
	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexprProducer);
	const IStatistics *stats = pexprProducer->Pstats();
	GPOS_ASSERT(NULL != stats);

	// copy the stats with the remaped colids
	IStatistics *new_stats = stats->CopyStatsWithRemap(mp, m_phmulcr);

	return new_stats;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEConsumer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalCTEConsumer::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]";
	return os;
}