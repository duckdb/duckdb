//---------------------------------------------------------------------------
//	@filename:
//		CPartFilterMap.cpp
//
//	@doc:
//		Implementation of partitioned table filter map
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CPartFilterMap.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/statistics/IStatistics.h"

#ifdef GPOS_DEBUG
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#endif	// GPOS_DEBUG

using namespace gpos;
using namespace gpopt;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilter::CPartFilter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartFilterMap::CPartFilter::CPartFilter(ULONG scan_id, CExpression *pexpr, IStatistics *stats)
	: m_scan_id(scan_id), m_pexpr(pexpr), m_pstats(stats)
{
	GPOS_ASSERT(NULL != pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilter::~CPartFilter
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartFilterMap::CPartFilter::~CPartFilter()
{
	m_pexpr->Release();
	CRefCount::SafeRelease(m_pstats);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilter::Matches
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
BOOL
CPartFilterMap::CPartFilter::Matches(const CPartFilter *ppf) const
{
	return NULL != ppf && m_scan_id == ppf->m_scan_id &&
		   m_pexpr->Matches(ppf->m_pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilter::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CPartFilterMap::CPartFilter::OsPrint(IOstream &os) const
{
	os << "[ " << m_scan_id << " : " << *m_pexpr << std::endl;
	if (NULL != m_pstats)
	{
		os << *m_pstats;
	}
	else
	{
		os << " (Empty Stats) ";
	}
	os << "]" << std::endl;

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilterMap
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartFilterMap::CPartFilterMap(CMemoryPool *mp)
{
	m_phmulpf = GPOS_NEW(mp) UlongToPartFilterMap(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilterMap
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartFilterMap::CPartFilterMap(CMemoryPool *mp, CPartFilterMap *ppfm)
{
	GPOS_ASSERT(NULL != ppfm);
	m_phmulpf = GPOS_NEW(mp) UlongToPartFilterMap(mp);
	CopyPartFilterMap(mp, ppfm);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::~CPartFilterMap
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartFilterMap::~CPartFilterMap()
{
	CRefCount::SafeRelease(m_phmulpf);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::FSubset
//
//	@doc:
//		Check if current part filter map is a subset of the given one
//
//---------------------------------------------------------------------------
BOOL
CPartFilterMap::FSubset(CPartFilterMap *ppfm)
{
	GPOS_ASSERT(NULL != ppfm);

	UlongToPartFilterMapIter hmulpfi(m_phmulpf);
	while (hmulpfi.Advance())
	{
		const CPartFilter *ppfCurrent = hmulpfi.Value();
		ULONG scan_id = ppfCurrent->ScanId();
		CPartFilter *ppfOther = ppfm->m_phmulpf->Find(&scan_id);
		if (!ppfCurrent->Matches(ppfOther))
		{
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::Pexpr
//
//	@doc:
//		Return the expression associated with the given scan id
//
//---------------------------------------------------------------------------
CExpression *
CPartFilterMap::Pexpr(ULONG scan_id) const
{
	CPartFilter *ppf = m_phmulpf->Find(&scan_id);
	GPOS_ASSERT(NULL != ppf);

	return ppf->Pexpr();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::Pstats
//
//	@doc:
//		Return the stats associated with the given scan id
//
//---------------------------------------------------------------------------
IStatistics *
CPartFilterMap::Pstats(ULONG scan_id) const
{
	CPartFilter *ppf = m_phmulpf->Find(&scan_id);
	GPOS_ASSERT(NULL != ppf);

	return ppf->Pstats();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::PdrgpulScanIds
//
//	@doc:
//		 Extract Scan ids
//
//---------------------------------------------------------------------------
ULongPtrArray *
CPartFilterMap::PdrgpulScanIds(CMemoryPool *mp) const
{
	ULongPtrArray *pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);
	UlongToPartFilterMapIter hmulpfi(m_phmulpf);
	while (hmulpfi.Advance())
	{
		CPartFilter *ppf = const_cast<CPartFilter *>(hmulpfi.Value());

		pdrgpul->Append(GPOS_NEW(mp) ULONG(ppf->ScanId()));
	}

	return pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::AddPartFilter
//
//	@doc:
//		Add part filter to map
//
//---------------------------------------------------------------------------
void
CPartFilterMap::AddPartFilter(CMemoryPool *mp, ULONG scan_id,
							  CExpression *pexpr, IStatistics *stats)
{
	GPOS_ASSERT(NULL != pexpr);
	CPartFilter *ppf = m_phmulpf->Find(&scan_id);
	if (NULL != ppf)
	{
		return;
	}

	ppf = GPOS_NEW(mp) CPartFilter(scan_id, pexpr, stats);

#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif	// GPOS_DEBUG
		m_phmulpf->Insert(GPOS_NEW(mp) ULONG(scan_id), ppf);

	GPOS_ASSERT(fSuccess);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::FCopyPartFilter
//
//	@doc:
//		Look for given scan id in given map and, if found, copy the
//		corresponding entry to current map,
//		return true if copying done successfully
//
//---------------------------------------------------------------------------
BOOL
CPartFilterMap::FCopyPartFilter(CMemoryPool *mp, ULONG scan_id,
								CPartFilterMap *ppfmSource,
								CColRefSet *filter_colrefs)
{
	GPOS_ASSERT(NULL != ppfmSource);
	GPOS_ASSERT(this != ppfmSource);

	CPartFilter *ppf = ppfmSource->m_phmulpf->Find(&scan_id);

	if (NULL != ppf)
	{
		if (NULL != filter_colrefs)
		{
			// separate the conjuncts in the filter and see if we can individually push down the
			// constraints that are semantically equivalent.
			CExpressionArray *pushable_conjuncts =
				GPOS_NEW(mp) CExpressionArray(mp);
			CExpressionArray *conjuncts =
				CPredicateUtils::PdrgpexprConjuncts(mp, ppf->Pexpr());

			for (ULONG ul = 0; ul < conjuncts->Size(); ++ul)
			{
				CExpression *conjunct = (*conjuncts)[ul];
				CColRefSet *pcrsUsed = conjunct->DeriveUsedColumns();
				if (filter_colrefs->ContainsAll(pcrsUsed))
				{
					conjunct->AddRef();
					pushable_conjuncts->Append(conjunct);
				}
			}
			conjuncts->Release();
			if (0 == pushable_conjuncts->Size())
			{
				pushable_conjuncts->Release();
				return false;
			}

			CExpression *pexprPushableFilter =
				CPredicateUtils::PexprConjunction(mp, pushable_conjuncts);
			ppf = GPOS_NEW(mp) CPartFilter(ppf->ScanId(), pexprPushableFilter);
		}
		else
		{
			ppf->AddRef();
		}
		ULONG *pulScanId = GPOS_NEW(mp) ULONG(scan_id);
		BOOL fSuccess = m_phmulpf->Insert(pulScanId, ppf);
		GPOS_ASSERT(fSuccess);

		if (!fSuccess)
		{
			ppf->Release();
			GPOS_DELETE(pulScanId);
		}

		return fSuccess;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CopyPartFilterMap
//
//	@doc:
//		Copy all part filters from source map to current map
//
//---------------------------------------------------------------------------
void
CPartFilterMap::CopyPartFilterMap(CMemoryPool *mp, CPartFilterMap *ppfmSource)
{
	GPOS_ASSERT(NULL != ppfmSource);
	GPOS_ASSERT(this != ppfmSource);

	UlongToPartFilterMapIter hmulpfi(ppfmSource->m_phmulpf);
	while (hmulpfi.Advance())
	{
		CPartFilter *ppf = const_cast<CPartFilter *>(hmulpfi.Value());
		ULONG scan_id = ppf->ScanId();

		// check if part filter with same scan id already exists
		if (NULL != m_phmulpf->Find(&scan_id))
		{
			continue;
		}

		ppf->AddRef();

#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif	// GPOS_DEBUG
			m_phmulpf->Insert(GPOS_NEW(mp) ULONG(scan_id), ppf);

		GPOS_ASSERT(fSuccess);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::OsPrint
//
//	@doc:
//		Print filter map
//
//---------------------------------------------------------------------------
IOstream &
CPartFilterMap::OsPrint(IOstream &os) const
{
	UlongToPartFilterMapIter hmulpfi(m_phmulpf);
	while (hmulpfi.Advance())
	{
		const CPartFilter *ppf = hmulpfi.Value();
		ppf->OsPrint(os);
	}

	return os;
}