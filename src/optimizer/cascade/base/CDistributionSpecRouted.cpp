//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpecRouted.cpp
//
//	@doc:
//		Specification of routed distribution
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDistributionSpecRouted.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionBroadcast.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionRoutedDistribute.h"
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::CDistributionSpecRouted
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecRouted::CDistributionSpecRouted(CColRef* pcrSegmentId)
	: m_pcrSegmentId(pcrSegmentId)
{
	GPOS_ASSERT(NULL != pcrSegmentId);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::~CDistributionSpecRouted
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDistributionSpecRouted::~CDistributionSpecRouted()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL CDistributionSpecRouted::FSatisfies(const CDistributionSpec* pds) const
{
	if (Matches(pds))
	{
		// exact match implies satisfaction
		return true;
	}
	if (EdtAny == pds->Edt())
	{
		// routed distribution satisfies the "any" distribution requirement
		return true;
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::PdsCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the distribution spec with remapped columns
//
//---------------------------------------------------------------------------
CDistributionSpec* CDistributionSpecRouted::PdsCopyWithRemappedColumns(CMemoryPool* mp, UlongToColRefMap* colref_mapping, BOOL must_exist)
{
	ULONG id = m_pcrSegmentId->Id();
	CColRef* pcrSegmentId = colref_mapping->Find(&id);
	if (NULL == pcrSegmentId)
	{
		if (must_exist)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			CColumnFactory* col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
			pcrSegmentId = col_factory->PcrCopy(m_pcrSegmentId);
			BOOL result = colref_mapping->Insert(GPOS_NEW(mp) ULONG(id), pcrSegmentId);
			GPOS_ASSERT(result);
		}
		else
		{
			pcrSegmentId = m_pcrSegmentId;
		}
	}
	return GPOS_NEW(mp) CDistributionSpecRouted(pcrSegmentId);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecRouted::AppendEnforcers(CMemoryPool* mp, CExpressionHandle &exprhdl, CReqdPropPlan*
#ifdef GPOS_DEBUG
											 prpp
#endif	// GPOS_DEBUG
										 , CExpressionArray* pdrgpexpr, CExpression* pexpr)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(this == prpp->Ped()->PdsRequired() && "required plan properties don't match enforced distribution spec");
	if (GPOS_FTRACE(EopttraceDisableMotionRountedDistribute))
	{
		// routed-distribute Motion is disabled
		return;
	}
	// add a routed distribution enforcer
	AddRef();
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPhysicalMotionRoutedDistribute(mp, this), pexpr);
	pdrgpexpr->Append(pexprMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDistributionSpecRouted::HashValue() const
{
	return gpos::CombineHashes((ULONG) Edt(),
							   gpos::HashPtr<CColRef>(m_pcrSegmentId));
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::PcrsUsed
//
//	@doc:
//		Extract columns used by the distribution spec
//
//---------------------------------------------------------------------------
CColRefSet* CDistributionSpecRouted::PcrsUsed(CMemoryPool* mp) const
{
	CColRefSet* pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_pcrSegmentId);
	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL CDistributionSpecRouted::Matches(const CDistributionSpec *pds) const
{
	if (Edt() != pds->Edt())
	{
		return false;
	}
	const CDistributionSpecRouted* pdsRouted = CDistributionSpecRouted::PdsConvert(pds);
	return m_pcrSegmentId == pdsRouted->Pcr();
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRouted::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream& CDistributionSpecRouted::OsPrint(IOstream &os) const
{
	os << "ROUTED: [ ";
	m_pcrSegmentId->OsPrint(os);
	return os << " ]";
}