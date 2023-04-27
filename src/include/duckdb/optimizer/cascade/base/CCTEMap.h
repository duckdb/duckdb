//---------------------------------------------------------------------------
//	@filename:
//		CCTEMap.h
//
//	@doc:
//		CTE map that is derived as part of plan properties. The map has
//		entries corresponding to unresolved CTEs (producers or consumers) in
//		the subtree under the physical operator in question. Note that if both
//		a consumer and a producer with the same ID appear in the subtree, they
//		cancel each other, and that CTE does not appear in the map for this
//		operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEMap_H
#define GPOPT_CCTEMap_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CHashMap.h"
#include "duckdb/optimizer/cascade/common/CHashMapIter.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CCTEInfo.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"

namespace gpopt
{
using namespace gpos;

// hash map from CTE id to corresponding producer plan properties
typedef CHashMap<ULONG, CDrvdPropPlan, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
				 CleanupRelease<CDrvdPropPlan> >
	UlongToDrvdPropPlanMap;

// iterator for plan properties map
typedef CHashMapIter<ULONG, CDrvdPropPlan, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupRelease<CDrvdPropPlan> >
	UlongToDrvdPropPlanMapIter;

// forward declaration
class CCTEReq;
//---------------------------------------------------------------------------
//	@class:
//		CCTEMap
//
//	@doc:
//		CTE map that is derived as part of plan properties
//
//---------------------------------------------------------------------------
class CCTEMap : public CRefCount
{
public:
	// CTE types
	enum ECteType
	{
		EctProducer,
		EctConsumer,
		EctSentinel
	};

private:
	//---------------------------------------------------------------------------
	//	@class:
	//		CCTEMapEntry
	//
	//	@doc:
	//		A single entry in the CTE map;
	//		Each entry has a CTE ID and whether it is a consumer or a producer.
	//		If entry corresponds to a producer, the entry also has properties of
	//		the plan rooted by producer node.
	//
	//---------------------------------------------------------------------------
	class CCTEMapEntry : public CRefCount
	{
	private:
		// cte id
		ULONG m_id;

		// cte type
		CCTEMap::ECteType m_ect;

		// derived plan properties if entry corresponds to CTE producer
		CDrvdPropPlan *m_pdpplan;

		// private copy ctor
		CCTEMapEntry(const CCTEMapEntry &);

	public:
		// ctor
		CCTEMapEntry(ULONG id, CCTEMap::ECteType ect, CDrvdPropPlan *pdpplan)
			: m_id(id), m_ect(ect), m_pdpplan(pdpplan)
		{
			GPOS_ASSERT(EctSentinel > ect);
			GPOS_ASSERT_IMP(EctProducer == ect, NULL != pdpplan);
		}

		// dtor
		virtual ~CCTEMapEntry()
		{
			CRefCount::SafeRelease(m_pdpplan);
		}

		// cte id
		ULONG
		Id() const
		{
			return m_id;
		}

		// cte type
		CCTEMap::ECteType
		Ect() const
		{
			return m_ect;
		}

		// plan properties
		CDrvdPropPlan *
		Pdpplan() const
		{
			return m_pdpplan;
		}

		// hash function
		ULONG
		HashValue() const
		{
			return gpos::CombineHashes(
				gpos::HashValue<ULONG>(&m_id),
				gpos::HashValue<CCTEMap::ECteType>(&m_ect));
		}

		// print function
		virtual IOstream &
		OsPrint(IOstream &os) const
		{
			os << m_id << (EctProducer == m_ect ? "p" : "c");
			if (NULL != m_pdpplan)
			{
				os << "(" << *m_pdpplan << ")";
			}
			return os;
		}


	};	// class CCTEMapEntry

	// map CTE id to CTE map entry
	typedef CHashMap<ULONG, CCTEMapEntry, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupRelease<CCTEMapEntry> >
		UlongToCTEMapEntryMap;

	// map iterator
	typedef CHashMapIter<ULONG, CCTEMapEntry, gpos::HashValue<ULONG>,
						 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
						 CleanupRelease<CCTEMapEntry> >
		UlongToCTEMapEntryMapIter;

	// memory pool
	CMemoryPool *m_mp;

	// cte map
	UlongToCTEMapEntryMap *m_phmcm;

	// private copy ctor
	CCTEMap(const CCTEMap &);

	// lookup info for given cte id
	CCTEMapEntry *PcmeLookup(ULONG ulCteId) const;

	// helper to add entries found in first map and are unresolved based on second map
	static void AddUnresolved(const CCTEMap &cmFirst, const CCTEMap &cmSecond,
							  CCTEMap *pcmResult);

public:
	// ctor
	explicit CCTEMap(CMemoryPool *mp);

	// dtor
	virtual ~CCTEMap();

	// return the CTE type associated with the given ID in the map
	ECteType Ect(const ULONG id) const;

	// inserting a new map entry, no entry with the same id can already exist
	void Insert(ULONG ulCteId, ECteType ect, CDrvdPropPlan *pdpplan);

	// hash function
	ULONG HashValue() const;

	// check if two cte maps are equal
	BOOL
	Equals(const CCTEMap *pcm) const
	{
		return (m_phmcm->Size() == pcm->m_phmcm->Size()) && this->FSubset(pcm);
	}

	// extract plan properties of the only producer in the map, if any
	CDrvdPropPlan *PdpplanProducer(ULONG *ulpId) const;

	// check if current  map is a subset of the given one
	BOOL FSubset(const CCTEMap *pcm) const;

	// check whether the current CTE map satisfies the given CTE requirements
	BOOL FSatisfies(const CCTEReq *pcter) const;

	// return producer ids that are in this map but not in the given requirement
	ULongPtrArray *PdrgpulAdditionalProducers(CMemoryPool *mp,
											  const CCTEReq *pcter) const;

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

	// combine the two given maps and return the resulting map
	static CCTEMap *PcmCombine(CMemoryPool *mp, const CCTEMap &cmFirst,
							   const CCTEMap &cmSecond);

};	// class CCTEMap

// shorthand for printing
IOstream &operator<<(IOstream &os, CCTEMap &cm);

}  // namespace gpopt

#endif
