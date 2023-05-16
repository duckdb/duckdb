//---------------------------------------------------------------------------
//	@filename:
//		CLogicalConstTableGet.cpp
//
//	@doc:
//		Implementation of const table access
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalConstTableGet.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/metadata/CName.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::CLogicalConstTableGet
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalConstTableGet::CLogicalConstTableGet(CMemoryPool *mp)
	: CLogical(mp), m_pdrgpcoldesc(NULL),  m_pdrgpdrgpdatum(NULL), m_pdrgpcrOutput(NULL)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::CLogicalConstTableGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalConstTableGet::CLogicalConstTableGet(CMemoryPool *mp, CColumnDescriptorArray *pdrgpcoldesc, IDatum2dArray *pdrgpdrgpdatum)
	: CLogical(mp), m_pdrgpcoldesc(pdrgpcoldesc), m_pdrgpdrgpdatum(pdrgpdrgpdatum), m_pdrgpcrOutput(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcoldesc);
	GPOS_ASSERT(NULL != pdrgpdrgpdatum);

	// generate a default column set for the list of column descriptors
	m_pdrgpcrOutput = PdrgpcrCreateMapping(mp, pdrgpcoldesc, UlOpId());

#ifdef GPOS_DEBUG
	for (ULONG ul = 0; ul < pdrgpdrgpdatum->Size(); ul++)
	{
		IDatumArray *pdrgpdatum = (*pdrgpdrgpdatum)[ul];
		GPOS_ASSERT(pdrgpdatum->Size() == pdrgpcoldesc->Size());
	}
#endif
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::CLogicalConstTableGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalConstTableGet::CLogicalConstTableGet(CMemoryPool *mp, CColRefArray *pdrgpcrOutput, IDatum2dArray *pdrgpdrgpdatum)
	: CLogical(mp), m_pdrgpcoldesc(NULL), m_pdrgpdrgpdatum(pdrgpdrgpdatum), m_pdrgpcrOutput(pdrgpcrOutput)
{
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpdrgpdatum);

	// generate column descriptors for the given output columns
	m_pdrgpcoldesc = PdrgpcoldescMapping(mp, pdrgpcrOutput);

#ifdef GPOS_DEBUG
	for (ULONG ul = 0; ul < pdrgpdrgpdatum->Size(); ul++)
	{
		IDatumArray *pdrgpdatum = (*pdrgpdrgpdatum)[ul];
		GPOS_ASSERT(pdrgpdatum->Size() == m_pdrgpcoldesc->Size());
	}
#endif
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::~CLogicalConstTableGet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalConstTableGet::~CLogicalConstTableGet()
{
	CRefCount::SafeRelease(m_pdrgpcoldesc);
	CRefCount::SafeRelease(m_pdrgpdrgpdatum);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalConstTableGet::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(
		COperator::HashValue(),
		gpos::CombineHashes(
			gpos::HashPtr<CColumnDescriptorArray>(m_pdrgpcoldesc),
			gpos::HashPtr<IDatum2dArray>(m_pdrgpdrgpdatum)));
	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalConstTableGet::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalConstTableGet *popCTG = CLogicalConstTableGet::PopConvert(pop);

	// match if column descriptors, const values and output columns are identical
	return m_pdrgpcoldesc->Equals(popCTG->Pdrgpcoldesc()) &&
		   m_pdrgpdrgpdatum->Equals(popCTG->Pdrgpdrgpdatum()) &&
		   m_pdrgpcrOutput->Equals(popCTG->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalConstTableGet::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRefArray *colref_array = NULL;
	if (must_exist)
	{
		colref_array =
			CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput, colref_mapping);
	}
	else
	{
		colref_array = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput, colref_mapping,
											must_exist);
	}
	m_pdrgpdrgpdatum->AddRef();

	return GPOS_NEW(mp)
		CLogicalConstTableGet(mp, colref_array, m_pdrgpdrgpdatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalConstTableGet::DeriveOutputColumns(CMemoryPool *mp,
										   CExpressionHandle &	// exprhdl
)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_pdrgpcrOutput);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalConstTableGet::DeriveMaxCard(CMemoryPool *,		  // mp
									 CExpressionHandle &  // exprhdl
) const
{
	return CMaxCard(m_pdrgpdrgpdatum->Size());
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalConstTableGet::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalConstTableGet::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementConstTableGet);
	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::PdrgpcoldescMapping
//
//	@doc:
//		Construct column descriptors from column references
//
//---------------------------------------------------------------------------
CColumnDescriptorArray *
CLogicalConstTableGet::PdrgpcoldescMapping(CMemoryPool *mp,
										   CColRefArray *colref_array) const
{
	GPOS_ASSERT(NULL != colref_array);
	CColumnDescriptorArray *pdrgpcoldesc =
		GPOS_NEW(mp) CColumnDescriptorArray(mp);

	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];

		ULONG length = gpos::ulong_max;
		if (CColRef::EcrtTable == colref->Ecrt())
		{
			CColRefTable *pcrTable = CColRefTable::PcrConvert(colref);
			length = pcrTable->Width();
		}

		CColumnDescriptor *pcoldesc = GPOS_NEW(mp) CColumnDescriptor(
			mp, colref->RetrieveType(), colref->TypeModifier(), colref->Name(),
			ul + 1,	 //attno
			true,	 // IsNullable
			length);
		pdrgpcoldesc->Append(pcoldesc);
	}

	return pdrgpcoldesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalConstTableGet::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
									IStatisticsArray *	// not used
) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	CReqdPropRelational *prprel =
		CReqdPropRelational::GetReqdRelationalProps(exprhdl.Prp());
	CColRefSet *pcrs = prprel->PcrsStat();
	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);
	pcrs->ExtractColIds(mp, colids);
	ULongPtrArray *pdrgpulColWidth = CUtils::Pdrgpul(mp, m_pdrgpcrOutput);

	IStatistics *stats = CStatistics::MakeDummyStats(
		mp, colids, pdrgpulColWidth, m_pdrgpdrgpdatum->Size());

	// clean up
	colids->Release();
	pdrgpulColWidth->Release();

	return stats;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalConstTableGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalConstTableGet::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	else
	{
		os << SzId() << " ";
		os << "Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
		os << "] ";
		os << "Values: [";
		for (ULONG ulA = 0; ulA < m_pdrgpdrgpdatum->Size(); ulA++)
		{
			if (0 < ulA)
			{
				os << "; ";
			}
			os << "(";
			IDatumArray *pdrgpdatum = (*m_pdrgpdrgpdatum)[ulA];

			const ULONG length = pdrgpdatum->Size();
			for (ULONG ulB = 0; ulB < length; ulB++)
			{
				IDatum *datum = (*pdrgpdatum)[ulB];
				datum->OsPrint(os);

				if (ulB < length - 1)
				{
					os << ", ";
				}
			}
			os << ")";
		}
		os << "]";
	}

	return os;
}