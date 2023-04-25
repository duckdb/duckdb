//---------------------------------------------------------------------------
//	@filename:
//		CPartIndexMap.h
//
//	@doc:
//		Mapping of partition index to a manipulator type
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartIndexMap_H
#define GPOPT_CPartIndexMap_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CHashMap.h"
#include "duckdb/optimizer/cascade/common/CHashMapIter.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CPartKeys.h"
#include "duckdb/optimizer/cascade/metadata/CPartConstraint.h"

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
class CPartitionPropagationSpec;

//---------------------------------------------------------------------------
//	@class:
//		CPartIndexMap
//
//	@doc:
//		Mapping of partition index to a manipulator type
//
//---------------------------------------------------------------------------
class CPartIndexMap : public CRefCount
{
public:
	// types of partition index id manipulators
	enum EPartIndexManipulator
	{
		EpimPropagator,
		EpimConsumer,
		EpimResolver,

		EpimSentinel
	};

	enum EPartPropagationRequestAction
	{
		EppraPreservePropagators,
		EppraIncrementPropagators,
		EppraZeroPropagators,
		EppraSentinel
	};

private:
	//---------------------------------------------------------------------------
	//	@class:
	//		CPartTableInfo
	//
	//	@doc:
	//		Partition index map entry
	//
	//---------------------------------------------------------------------------
	class CPartTableInfo : public CRefCount
	{
	private:
		// scan id
		ULONG m_scan_id;

		// part constraints for partial scans and partition resolvers indexed
		// by the scan id
		UlongToPartConstraintMap *m_ppartcnstrmap;

		// manipulator type
		EPartIndexManipulator m_epim;

		// does this part table item contain partial scans
		BOOL m_fPartialScans;

		// partition table mdid
		IMDId *m_mdid;

		// partition keys
		CPartKeysArray *m_pdrgppartkeys;

		// part constraint of the relation
		CPartConstraint *m_ppartcnstrRel;

		// number of propagators to expect - this is only valid if the
		// manipulator type is Consumer
		ULONG m_ulPropagators;

		// description of manipulator types
		static const CHAR *m_szManipulator[EpimSentinel];

		// add a part constraint
		void AddPartConstraint(CMemoryPool *mp, ULONG part_idx_id,
							   CPartConstraint *ppartcnstr);

		// private copy ctor
		CPartTableInfo(const CPartTableInfo &);

		// does the given part constraint map define partial scans
		static BOOL FDefinesPartialScans(
			UlongToPartConstraintMap *ppartcnstrmap,
			CPartConstraint *ppartcnstrRel);

	public:
		// ctor
		CPartTableInfo(ULONG scan_id, UlongToPartConstraintMap *ppartcnstrmap,
					   EPartIndexManipulator epim, IMDId *mdid,
					   CPartKeysArray *pdrgppartkeys,
					   CPartConstraint *ppartcnstrRel, ULONG ulPropagators);

		//dtor
		virtual ~CPartTableInfo();

		// partition index accessor
		virtual ULONG
		ScanId() const
		{
			return m_scan_id;
		}

		// part constraint map accessor
		UlongToPartConstraintMap *
		Ppartcnstrmap() const
		{
			return m_ppartcnstrmap;
		}

		// relation part constraint
		CPartConstraint *
		PpartcnstrRel() const
		{
			return m_ppartcnstrRel;
		}

		// expected number of propagators
		ULONG
		UlExpectedPropagators() const
		{
			return m_ulPropagators;
		}

		// set the number of expected propagators
		void
		SetExpectedPropagators(ULONG ulPropagators)
		{
			m_ulPropagators = ulPropagators;
		}

		// manipulator type accessor
		virtual EPartIndexManipulator
		Epim() const
		{
			return m_epim;
		}

		// partial scans accessor
		virtual BOOL
		FPartialScans() const
		{
			return m_fPartialScans;
		}

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

		static const CHAR *SzManipulatorType(EPartIndexManipulator epim);

		// add part constraints
		void AddPartConstraints(CMemoryPool *mp,
								UlongToPartConstraintMap *ppartcnstrmap);

		virtual IOstream &OsPrint(IOstream &os) const;

	};	// CPartTableInfo

	// map scan id to partition table info entry
	typedef CHashMap<ULONG, CPartTableInfo, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupRelease<CPartTableInfo> >
		ScanIdToPartTableInfoMap;

	// map iterator
	typedef CHashMapIter<ULONG, CPartTableInfo, gpos::HashValue<ULONG>,
						 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
						 CleanupRelease<CPartTableInfo> >
		ScanIdToPartTableInfoMapIter;

	// memory pool
	CMemoryPool *m_mp;

	// partition index map
	ScanIdToPartTableInfoMap *m_pim;

	// number of unresolved entries
	ULONG m_ulUnresolved;

	// number of unresolved entries with zero expected propagators
	ULONG m_ulUnresolvedZeroPropagators;

	// private copy ctor
	CPartIndexMap(const CPartIndexMap &);

	// lookup info for given scan id
	CPartTableInfo *PptiLookup(ULONG scan_id) const;

	// check if part index map entry satisfies the corresponding required
	// partition propagation spec entry
	BOOL FSatisfiesEntry(const CPartTableInfo *pptiReqd,
						 CPartTableInfo *pptiDrvd) const;

	// handle the cases where one of the given manipulators is a propagator and the other is a consumer
	static void ResolvePropagator(EPartIndexManipulator epimFst,
								  ULONG ulExpectedPropagatorsFst,
								  EPartIndexManipulator epimSnd,
								  ULONG ulExpectedPropagatorsSnd,
								  EPartIndexManipulator *pepimResult,
								  ULONG *pulExpectedPropagatorsResult);

	// helper to add part-index id's found in first map and are unresolved based on second map
	static void AddUnresolved(CMemoryPool *mp, const CPartIndexMap &pimFst,
							  const CPartIndexMap &pimSnd,
							  CPartIndexMap *ppimResult);

	// print part constraint map
	static IOstream &OsPrintPartCnstrMap(
		ULONG part_idx_id, UlongToPartConstraintMap *ppartcnstrmap,
		IOstream &os);

public:
	// ctor
	explicit CPartIndexMap(CMemoryPool *mp);

	// dtor
	virtual ~CPartIndexMap();

	// inserting a new map entry
	void Insert(ULONG scan_id, UlongToPartConstraintMap *ppartcnstrmap,
				EPartIndexManipulator epim, ULONG ulExpectedPropagators,
				IMDId *mdid, CPartKeysArray *pdrgppartkeys,
				CPartConstraint *ppartcnstrRel);

	// does map contain unresolved entries?
	BOOL FContainsUnresolved() const;

	// does map contain unresolved entries with zero propagators?
	BOOL FContainsUnresolvedZeroPropagators() const;

	// extract scan ids in the given memory pool
	ULongPtrArray *PdrgpulScanIds(CMemoryPool *mp,
								  BOOL fConsumersOnly = false) const;

	// check if two part index maps are equal
	BOOL
	Equals(const CPartIndexMap *ppim) const
	{
		return (m_pim->Size() == ppim->m_pim->Size()) && ppim->FSubset(this);
	}

	// hash function
	ULONG HashValue() const;

	// check if partition index map satsfies required partition propagation spec
	BOOL FSatisfies(const CPartitionPropagationSpec *ppps) const;

	// check if current part index map is a subset of the given one
	BOOL FSubset(const CPartIndexMap *ppim) const;

	// check if part index map contains the given scan id
	BOOL
	Contains(ULONG scan_id) const
	{
		return NULL != m_pim->Find(&scan_id);
	}

	// check if the given expression derives unneccessary partition selectors
	BOOL FContainsRedundantPartitionSelectors(CPartIndexMap *ppimReqd) const;

	// part keys of the entry with the given scan id
	CPartKeysArray *Pdrgppartkeys(ULONG scan_id) const;

	// relation mdid of the entry with the given scan id
	IMDId *GetRelMdId(ULONG scan_id) const;

	// part constraint map of the entry with the given scan id
	UlongToPartConstraintMap *Ppartcnstrmap(ULONG scan_id) const;

	// relation part constraint of the entry with the given scan id
	CPartConstraint *PpartcnstrRel(ULONG scan_id) const;

	// manipulator type of the entry with the given scan id
	EPartIndexManipulator Epim(ULONG scan_id) const;

	// number of expected propagators of the entry with the given scan id
	ULONG UlExpectedPropagators(ULONG scan_id) const;

	// set the number of expected propagators for the entry with the given scan id
	void SetExpectedPropagators(ULONG scan_id, ULONG ulPropagators);

	// check whether the entry with the given scan id has partial scans
	BOOL FPartialScans(ULONG scan_id) const;

	// get part consumer with given scanId from the given map, and add it to the
	// current map with the given array of keys
	void AddRequiredPartPropagation(CPartIndexMap *ppimSource, ULONG scan_id,
									EPartPropagationRequestAction eppra,
									CPartKeysArray *pdrgppartkeys = NULL);

	// return a new part index map for a partition selector with the given
	// scan id, and the given number of expected selectors above it
	CPartIndexMap *PpimPartitionSelector(CMemoryPool *mp, ULONG scan_id,
										 ULONG ulExpectedFromReq) const;

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

	// combine the two given maps and return the resulting map
	static CPartIndexMap *PpimCombine(CMemoryPool *mp,
									  const CPartIndexMap &pimFst,
									  const CPartIndexMap &pimSnd);

};	// class CPartIndexMap

// shorthand for printing
IOstream &operator<<(IOstream &os, CPartIndexMap &pim);

}  // namespace gpopt

#endif
