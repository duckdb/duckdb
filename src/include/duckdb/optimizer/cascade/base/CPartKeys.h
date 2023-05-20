//---------------------------------------------------------------------------
//	@filename:
//		CPartKeys.h
//
//	@doc:
//		A collection of partitioning keys for a partitioned table
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartKeys_H
#define GPOPT_CPartKeys_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"

namespace gpopt
{
using namespace gpos;

// fwd decl
class CColRefSet;
class CPartKeys;

// array of part keys
typedef CDynamicPtrArray<CPartKeys, CleanupRelease> CPartKeysArray;

//---------------------------------------------------------------------------
//	@class:
//		CPartKeys
//
//	@doc:
//		A collection of partitioning keys for a partitioned table
//
//---------------------------------------------------------------------------
class CPartKeys : public CRefCount
{
private:
	// partitioning keys
	CColRef2dArray *m_pdrgpdrgpcr;

	// number of levels
	ULONG m_num_of_part_levels;

	// private copy ctor
	CPartKeys(const CPartKeys &);

public:
	// ctor
	explicit CPartKeys(CColRef2dArray *pdrgpdrgpcr);

	// dtor
	~CPartKeys();

	// return key at a given level
	CColRef *PcrKey(ULONG ulLevel) const;

	// return array of keys
	CColRef2dArray *
	Pdrgpdrgpcr() const
	{
		return m_pdrgpdrgpcr;
	}

	// number of levels
	ULONG
	GetPartitioningLevel() const
	{
		return m_num_of_part_levels;
	}

	// copy part key into the given memory pool
	CPartKeys *PpartkeysCopy(CMemoryPool *mp);

	// check whether the key columns overlap the given column
	BOOL FOverlap(CColRefSet *pcrs) const;

	// create a new PartKeys object from the current one by remapping the
	// keys using the given hashmap
	CPartKeys *PpartkeysRemap(CMemoryPool *mp,
							  UlongToColRefMap *colref_mapping) const;

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// copy array of part keys into given memory pool
	static CPartKeysArray *PdrgppartkeysCopy(
		CMemoryPool *mp, const CPartKeysArray *pdrgppartkeys);

};	// CPartKeys

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CPartKeys &partkeys)
{
	return partkeys.OsPrint(os);
}

}  // namespace gpopt

#endif
