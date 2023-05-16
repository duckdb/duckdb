//---------------------------------------------------------------------------
//	@filename:
//		CLogicalGet.cpp
//
//	@doc:
//		Implementation of basic table access
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalGet.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CAutoP.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/metadata/CName.h"
#include "duckdb/optimizer/cascade/metadata/CTableDescriptor.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::CLogicalGet
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalGet::CLogicalGet(CMemoryPool *mp)
	: CLogical(mp),
	  m_pnameAlias(NULL),
	  m_ptabdesc(NULL),
	  m_pdrgpcrOutput(NULL),
	  m_pdrgpdrgpcrPart(NULL),
	  m_pcrsDist(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::CLogicalGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalGet::CLogicalGet(CMemoryPool *mp, const CName *pnameAlias,
						 CTableDescriptor *ptabdesc)
	: CLogical(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdesc(ptabdesc),
	  m_pdrgpcrOutput(NULL),
	  m_pdrgpdrgpcrPart(NULL),
	  m_pcrsDist(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);

	// generate a default column set for the table descriptor
	m_pdrgpcrOutput = PdrgpcrCreateMapping(mp, m_ptabdesc->Pdrgpcoldesc(),
										   UlOpId(), m_ptabdesc->MDId());

	if (m_ptabdesc->IsPartitioned())
	{
		m_pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(
			mp, m_pdrgpcrOutput, m_ptabdesc->PdrgpulPart());
	}

	m_pcrsDist = CLogical::PcrsDist(mp, m_ptabdesc, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::CLogicalGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalGet::CLogicalGet(CMemoryPool *mp, const CName *pnameAlias,
						 CTableDescriptor *ptabdesc,
						 CColRefArray *pdrgpcrOutput)
	: CLogical(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdesc(ptabdesc),
	  m_pdrgpcrOutput(pdrgpcrOutput),
	  m_pdrgpdrgpcrPart(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);

	if (m_ptabdesc->IsPartitioned())
	{
		m_pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(
			mp, m_pdrgpcrOutput, m_ptabdesc->PdrgpulPart());
	}

	m_pcrsDist = CLogical::PcrsDist(mp, m_ptabdesc, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::~CLogicalGet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalGet::~CLogicalGet()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrPart);
	CRefCount::SafeRelease(m_pcrsDist);

	GPOS_DELETE(m_pnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalGet::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   m_ptabdesc->MDId()->HashValue());
	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalGet::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	CLogicalGet *popGet = CLogicalGet::PopConvert(pop);

	return m_ptabdesc->MDId()->Equals(popGet->m_ptabdesc->MDId()) &&
		   m_pdrgpcrOutput->Equals(popGet->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalGet::PopCopyWithRemappedColumns(CMemoryPool *mp,
										UlongToColRefMap *colref_mapping,
										BOOL must_exist)
{
	CColRefArray *pdrgpcrOutput = NULL;
	if (must_exist)
	{
		pdrgpcrOutput =
			CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput, colref_mapping);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput,
											 colref_mapping, must_exist);
	}
	CName *pnameAlias = GPOS_NEW(mp) CName(mp, *m_pnameAlias);
	m_ptabdesc->AddRef();

	return GPOS_NEW(mp) CLogicalGet(mp, pnameAlias, m_ptabdesc, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGet::DeriveOutputColumns(CMemoryPool *mp,
								 CExpressionHandle &  // exprhdl
)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	for (ULONG i = 0; i < m_pdrgpcrOutput->Size(); i++)
	{
		// We want to limit the output columns to only those which are referenced in the query
		// We will know the entire list of columns which are referenced in the query only after
		// translating the entire DXL to an expression. Hence we should not limit the output columns
		// before we have processed the entire DXL.
		if ((*m_pdrgpcrOutput)[i]->GetUsage() == CColRef::EUsed ||
			(*m_pdrgpcrOutput)[i]->GetUsage() == CColRef::EUnknown)
		{
			pcrs->Include((*m_pdrgpcrOutput)[i]);
		}
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::DeriveNotNullColumns
//
//	@doc:
//		Derive not null output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGet::DeriveNotNullColumns(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const
{
	// get all output columns
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(exprhdl.DeriveOutputColumns());

	// filters out nullable columns
	CColRefSetIter crsi(*exprhdl.DeriveOutputColumns());
	while (crsi.Advance())
	{
		CColRefTable *pcrtable =
			CColRefTable::PcrConvert(const_cast<CColRef *>(crsi.Pcr()));
		if (pcrtable->IsNullable())
		{
			pcrs->Exclude(pcrtable);
		}
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalGet::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalGet::DeriveKeyCollection(CMemoryPool *mp,
								 CExpressionHandle &  // exprhdl
) const
{
	const CBitSetArray *pdrgpbs = m_ptabdesc->PdrgpbsKeys();

	return CLogical::PkcKeysBaseTable(mp, pdrgpbs, m_pdrgpcrOutput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalGet::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfGet2TableScan);

	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::PstatsDerive
//
//	@doc:
//		Load up statistics from metadata
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalGet::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  IStatisticsArray *  // not used
) const
{
	// requesting stats on distribution columns to estimate data skew
	IStatistics *pstatsTable =
		PstatsBaseTable(mp, exprhdl, m_ptabdesc, m_pcrsDist);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcrOutput);
	CUpperBoundNDVs *upper_bound_NDVs =
		GPOS_NEW(mp) CUpperBoundNDVs(pcrs, pstatsTable->Rows());
	CStatistics::CastStats(pstatsTable)->AddCardUpperBound(upper_bound_NDVs);

	return pstatsTable;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalGet::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	else
	{
		os << SzId() << " ";
		// alias of table as referenced in the query
		m_pnameAlias->OsPrint(os);

		// actual name of table in catalog and columns
		os << " (";
		m_ptabdesc->Name().OsPrint(os);
		os << "), Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
		os << "] Key sets: {";

		const ULONG ulColumns = m_pdrgpcrOutput->Size();
		const CBitSetArray *pdrgpbsKeys = m_ptabdesc->PdrgpbsKeys();
		for (ULONG ul = 0; ul < pdrgpbsKeys->Size(); ul++)
		{
			CBitSet *pbs = (*pdrgpbsKeys)[ul];
			if (0 < ul)
			{
				os << ", ";
			}
			os << "[";
			ULONG ulPrintedKeys = 0;
			for (ULONG ulKey = 0; ulKey < ulColumns; ulKey++)
			{
				if (pbs->Get(ulKey))
				{
					if (0 < ulPrintedKeys)
					{
						os << ",";
					}
					os << ulKey;
					ulPrintedKeys++;
				}
			}
			os << "]";
		}
		os << "}";
	}

	return os;
}