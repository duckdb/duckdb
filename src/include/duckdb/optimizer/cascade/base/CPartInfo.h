//---------------------------------------------------------------------------
//	@filename:
//		CPartInfo.h
//
//	@doc:
//		Derived partition information at the logical level
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartInfo_H
#define GPOPT_CPartInfo_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CPartKeys.h"

// fwd decl
namespace gpmd
{
class IMDId;
}

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

// fwd decl
class CPartConstraint;

//---------------------------------------------------------------------------
//	@class:
//		CPartInfo
//
//	@doc:
//		Derived partition information at the logical level
//
//---------------------------------------------------------------------------
class CPartInfo : public CRefCount
{
private:
	//---------------------------------------------------------------------------
	//	@class:
	//		CPartInfoEntry
	//
	//	@doc:
	//		A single entry of the CPartInfo
	//
	//---------------------------------------------------------------------------
	class CPartInfoEntry : public CRefCount
	{
	private:
		// scan id
		ULONG m_scan_id;

		// partition table mdid
		IMDId *m_mdid;

		// partition keys
		CPartKeysArray *m_pdrgppartkeys;

		// part constraint of the relation
		CPartConstraint *m_ppartcnstrRel;

		// private copy ctor
		CPartInfoEntry(const CPartInfoEntry &);

	public:
		// ctor
		CPartInfoEntry(ULONG scan_id, IMDId *mdid,
					   CPartKeysArray *pdrgppartkeys,
					   CPartConstraint *ppartcnstrRel);

		// dtor
		virtual ~CPartInfoEntry();

		// scan id
		virtual ULONG
		ScanId() const
		{
			return m_scan_id;
		}

		// relation part constraint
		CPartConstraint *
		PpartcnstrRel() const
		{
			return m_ppartcnstrRel;
		}

		// create a copy of the current object, and add a set of remapped
		// part keys to this entry, using the existing keys and the given hashmap
		CPartInfoEntry *PpartinfoentryAddRemappedKeys(
			CMemoryPool *mp, CColRefSet *pcrs,
			UlongToColRefMap *colref_mapping);

		// mdid of partition table
		virtual IMDId *
		MDId() const
		{
			return m_mdid;
		}

		// partition keys of partition table
		virtual CPartKeysArray *
		Pdrgppartkeys() const
		{
			return m_pdrgppartkeys;
		}

		// print function
		virtual IOstream &OsPrint(IOstream &os) const;

		// copy part info entry into given memory pool
		CPartInfoEntry *PpartinfoentryCopy(CMemoryPool *mp);

	};	// CPartInfoEntry

	typedef CDynamicPtrArray<CPartInfoEntry, CleanupRelease>
		CPartInfoEntryArray;

	// partition table consumers
	CPartInfoEntryArray *m_pdrgppartentries;

	// private ctor
	explicit CPartInfo(CPartInfoEntryArray *pdrgppartentries);

	//private copy ctor
	CPartInfo(const CPartInfo &);

public:
	// ctor
	explicit CPartInfo(CMemoryPool *mp);

	// dtor
	virtual ~CPartInfo();

	// number of part table consumers
	ULONG
	UlConsumers() const
	{
		return m_pdrgppartentries->Size();
	}

	// add part table consumer
	void AddPartConsumer(CMemoryPool *mp, ULONG scan_id, IMDId *mdid,
						 CColRef2dArray *pdrgpdrgpcrPart,
						 CPartConstraint *ppartcnstrRel);

	// scan id of the entry at the given position
	ULONG ScanId(ULONG ulPos) const;

	// relation mdid of the entry at the given position
	IMDId *GetRelMdId(ULONG ulPos) const;

	// part keys of the entry at the given position
	CPartKeysArray *Pdrgppartkeys(ULONG ulPos) const;

	// part constraint of the entry at the given position
	CPartConstraint *Ppartcnstr(ULONG ulPos) const;

	// check if part info contains given scan id
	BOOL FContainsScanId(ULONG scan_id) const;

	// part keys of the entry with the given scan id
	CPartKeysArray *PdrgppartkeysByScanId(ULONG scan_id) const;

	// return a new part info object with an additional set of remapped keys
	CPartInfo *PpartinfoWithRemappedKeys(CMemoryPool *mp,
										 CColRefArray *pdrgpcrSrc,
										 CColRefArray *pdrgpcrDest) const;

	// print
	virtual IOstream &OsPrint(IOstream &) const;

	// combine two part info objects
	static CPartInfo *PpartinfoCombine(CMemoryPool *mp, CPartInfo *ppartinfoFst,
									   CPartInfo *ppartinfoSnd);

};	// CPartInfo

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CPartInfo &partinfo)
{
	return partinfo.OsPrint(os);
}
}  // namespace gpopt

#endif
