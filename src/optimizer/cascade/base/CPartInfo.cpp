//---------------------------------------------------------------------------
//	@filename:
//		CPartInfo.cpp
//
//	@doc:
//		Implementation of derived partition information at the logical level
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CPartInfo.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/metadata/CPartConstraint.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::CPartInfoEntry::CPartInfoEntry
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartInfo::CPartInfoEntry::CPartInfoEntry(ULONG scan_id, IMDId *mdid, CPartKeysArray *pdrgppartkeys, CPartConstraint *ppartcnstrRel)
	: m_scan_id(scan_id), m_mdid(mdid), m_pdrgppartkeys(pdrgppartkeys), m_ppartcnstrRel(ppartcnstrRel)
{
	GPOS_ASSERT(mdid->IsValid());
	GPOS_ASSERT(pdrgppartkeys != NULL);
	GPOS_ASSERT(0 < pdrgppartkeys->Size());
	GPOS_ASSERT(NULL != ppartcnstrRel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::CPartInfoEntry::~CPartInfoEntry
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartInfo::CPartInfoEntry::~CPartInfoEntry()
{
	m_mdid->Release();
	m_pdrgppartkeys->Release();
	m_ppartcnstrRel->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::CPartInfoEntry::PpartinfoentryAddRemappedKeys
//
//	@doc:
//		Create a copy of the current object, and add a set of remapped
//		part keys to this entry, using the existing keys and the given hashmap
//
//---------------------------------------------------------------------------
CPartInfo::CPartInfoEntry* CPartInfo::CPartInfoEntry::PpartinfoentryAddRemappedKeys(CMemoryPool *mp, CColRefSet *pcrs, UlongToColRefMap *colref_mapping)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != colref_mapping);
	CPartKeysArray *pdrgppartkeys = CPartKeys::PdrgppartkeysCopy(mp, m_pdrgppartkeys);
	const ULONG size = m_pdrgppartkeys->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CPartKeys *ppartkeys = (*m_pdrgppartkeys)[ul];
		if (ppartkeys->FOverlap(pcrs))
		{
			pdrgppartkeys->Append(ppartkeys->PpartkeysRemap(mp, colref_mapping));
			break;
		}
	}
	m_mdid->AddRef();
	CPartConstraint *ppartcnstrRel = m_ppartcnstrRel->PpartcnstrCopyWithRemappedColumns(mp, colref_mapping, false);
	return GPOS_NEW(mp) CPartInfoEntry(m_scan_id, m_mdid, pdrgppartkeys, ppartcnstrRel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::CPartInfoEntry::OsPrint
//
//	@doc:
//		Print functions
//
//---------------------------------------------------------------------------
IOstream & CPartInfo::CPartInfoEntry::OsPrint(IOstream &os) const
{
	os << m_scan_id;
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::CPartInfoEntry::PpartinfoentryCopy
//
//	@doc:
//		Copy part info entry into given memory pool
//
//---------------------------------------------------------------------------
CPartInfo::CPartInfoEntry* CPartInfo::CPartInfoEntry::PpartinfoentryCopy(CMemoryPool *mp)
{
	IMDId *mdid = MDId();
	mdid->AddRef();
	// copy part keys
	CPartKeysArray *pdrgppartkeysCopy = CPartKeys::PdrgppartkeysCopy(mp, Pdrgppartkeys());
	// copy part constraint using empty remapping to get exact copy
	UlongToColRefMap *colref_mapping = GPOS_NEW(mp) UlongToColRefMap(mp);
	CPartConstraint *ppartcnstrRel = PpartcnstrRel()->PpartcnstrCopyWithRemappedColumns(mp, colref_mapping, false /*must_exist*/);
	colref_mapping->Release();
	return GPOS_NEW(mp) CPartInfoEntry(ScanId(), mdid, pdrgppartkeysCopy, ppartcnstrRel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::CPartInfo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartInfo::CPartInfo(CPartInfoEntryArray *pdrgppartentries)
	: m_pdrgppartentries(pdrgppartentries)
{
	GPOS_ASSERT(NULL != pdrgppartentries);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::CPartInfo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartInfo::CPartInfo(CMemoryPool *mp)
{
	m_pdrgppartentries = GPOS_NEW(mp) CPartInfoEntryArray(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::~CPartInfo
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartInfo::~CPartInfo()
{
	CRefCount::SafeRelease(m_pdrgppartentries);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::AddPartConsumer
//
//	@doc:
//		Add part table consumer
//
//---------------------------------------------------------------------------
void CPartInfo::AddPartConsumer(CMemoryPool *mp, ULONG scan_id, IMDId *mdid, CColRef2dArray *pdrgpdrgpcrPart, CPartConstraint *ppartcnstrRel)
{
	CPartKeysArray *pdrgppartkeys = GPOS_NEW(mp) CPartKeysArray(mp);
	pdrgppartkeys->Append(GPOS_NEW(mp) CPartKeys(pdrgpdrgpcrPart));
	m_pdrgppartentries->Append(GPOS_NEW(mp) CPartInfoEntry(scan_id, mdid, pdrgppartkeys, ppartcnstrRel));
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::FContainsScanId
//
//	@doc:
//		Check if part info contains given scan id
//
//---------------------------------------------------------------------------
BOOL CPartInfo::FContainsScanId(ULONG scan_id) const
{
	const ULONG size = m_pdrgppartentries->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CPartInfoEntry* ppartinfoentry = (*m_pdrgppartentries)[ul];
		if (scan_id == ppartinfoentry->ScanId())
		{
			return true;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::ScanId
//
//	@doc:
//		Return scan id of the entry at the given position
//
//---------------------------------------------------------------------------
ULONG CPartInfo::ScanId(ULONG ulPos) const
{
	return (*m_pdrgppartentries)[ulPos]->ScanId();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::GetRelMdId
//
//	@doc:
//		Return relation mdid of the entry at the given position
//
//---------------------------------------------------------------------------
IMDId* CPartInfo::GetRelMdId(ULONG ulPos) const
{
	return (*m_pdrgppartentries)[ulPos]->MDId();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::Pdrgppartkeys
//
//	@doc:
//		Return part keys of the entry at the given position
//
//---------------------------------------------------------------------------
CPartKeysArray* CPartInfo::Pdrgppartkeys(ULONG ulPos) const
{
	return (*m_pdrgppartentries)[ulPos]->Pdrgppartkeys();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::Ppartcnstr
//
//	@doc:
//		Return part constraint of the entry at the given position
//
//---------------------------------------------------------------------------
CPartConstraint* CPartInfo::Ppartcnstr(ULONG ulPos) const
{
	return (*m_pdrgppartentries)[ulPos]->PpartcnstrRel();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::PdrgppartkeysByScanId
//
//	@doc:
//		Return part keys of the entry with the given scan id
//
//---------------------------------------------------------------------------
CPartKeysArray* CPartInfo::PdrgppartkeysByScanId(ULONG scan_id) const
{
	const ULONG size = m_pdrgppartentries->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CPartInfoEntry *ppartinfoentry = (*m_pdrgppartentries)[ul];
		if (scan_id == ppartinfoentry->ScanId())
		{
			return ppartinfoentry->Pdrgppartkeys();
		}
	}
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::PpartinfoWithRemappedKeys
//
//	@doc:
//		Return a new part info object with an additional set of remapped keys
//
//---------------------------------------------------------------------------
CPartInfo* CPartInfo::PpartinfoWithRemappedKeys(CMemoryPool *mp, CColRefArray *pdrgpcrSrc, CColRefArray *pdrgpcrDest) const
{
	GPOS_ASSERT(NULL != pdrgpcrSrc);
	GPOS_ASSERT(NULL != pdrgpcrDest);
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrSrc);
	UlongToColRefMap *colref_mapping = CUtils::PhmulcrMapping(mp, pdrgpcrSrc, pdrgpcrDest);
	CPartInfoEntryArray *pdrgppartentries = GPOS_NEW(mp) CPartInfoEntryArray(mp);
	const ULONG size = m_pdrgppartentries->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CPartInfoEntry *ppartinfoentry = (*m_pdrgppartentries)[ul];
		// if this entry has keys that overlap with the source columns then
		// add another set of keys to it using the destination columns
		CPartInfoEntry *ppartinfoentryNew = ppartinfoentry->PpartinfoentryAddRemappedKeys(mp, pcrs, colref_mapping);
		pdrgppartentries->Append(ppartinfoentryNew);
	}
	pcrs->Release();
	colref_mapping->Release();
	return GPOS_NEW(mp) CPartInfo(pdrgppartentries);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::PpartinfoCombine
//
//	@doc:
//		Combine two part info objects
//
//---------------------------------------------------------------------------
CPartInfo* CPartInfo::PpartinfoCombine(CMemoryPool *mp, CPartInfo *ppartinfoFst, CPartInfo *ppartinfoSnd)
{
	GPOS_ASSERT(NULL != ppartinfoFst);
	GPOS_ASSERT(NULL != ppartinfoSnd);
	CPartInfoEntryArray *pdrgppartentries = GPOS_NEW(mp) CPartInfoEntryArray(mp);
	// copy part entries from first part info object
	CUtils::AddRefAppend(pdrgppartentries, ppartinfoFst->m_pdrgppartentries);
	// copy part entries from second part info object, except those which already exist
	const ULONG length = ppartinfoSnd->m_pdrgppartentries->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CPartInfoEntry *ppartinfoentry = (*(ppartinfoSnd->m_pdrgppartentries))[ul];
		CPartKeysArray *pdrgppartkeys = ppartinfoFst->PdrgppartkeysByScanId(ppartinfoentry->ScanId());
		if (NULL != pdrgppartkeys)
		{
			// there is already an entry with the same scan id; need to add to it
			// the keys from the current entry
			CPartKeysArray *pdrgppartkeysCopy = CPartKeys::PdrgppartkeysCopy(mp, ppartinfoentry->Pdrgppartkeys());
			CUtils::AddRefAppend(pdrgppartkeys, pdrgppartkeysCopy);
			pdrgppartkeysCopy->Release();
		}
		else
		{
			CPartInfoEntry *ppartinfoentryCopy = ppartinfoentry->PpartinfoentryCopy(mp);
			pdrgppartentries->Append(ppartinfoentryCopy);
		}
	}
	return GPOS_NEW(mp) CPartInfo(pdrgppartentries);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartInfo::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream & CPartInfo::OsPrint(IOstream &os) const
{
	const ULONG length = m_pdrgppartentries->Size();
	os << "Part Consumers: ";
	for (ULONG ul = 0; ul < length; ul++)
	{
		CPartInfoEntry *ppartinfoentry = (*m_pdrgppartentries)[ul];
		ppartinfoentry->OsPrint(os);
		// separator
		os << (ul == length - 1 ? "" : ", ");
	}
	os << ", Part Keys: ";
	for (ULONG ulCons = 0; ulCons < length; ulCons++)
	{
		CPartKeysArray *pdrgppartkeys = Pdrgppartkeys(ulCons);
		os << "(";
		const ULONG ulPartKeys = pdrgppartkeys->Size();
		;
		for (ULONG ulPartKey = 0; ulPartKey < ulPartKeys; ulPartKey++)
		{
			os << *(*pdrgppartkeys)[ulPartKey];
			os << (ulPartKey == ulPartKeys - 1 ? "" : ", ");
		}
		os << ")";
		os << (ulCons == length - 1 ? "" : ", ");
	}
	return os;
}