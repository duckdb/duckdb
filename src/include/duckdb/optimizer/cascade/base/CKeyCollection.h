//---------------------------------------------------------------------------
//	@filename:
//		CKeyCollection.h
//
//	@doc:
//		Encodes key sets for a relation
//---------------------------------------------------------------------------
#ifndef GPOPT_CKeyCollection_H
#define GPOPT_CKeyCollection_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CColRefSet.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CKeyCollection
//
//	@doc:
//		Captures sets of keys for a relation
//
//---------------------------------------------------------------------------
class CKeyCollection : public CRefCount
{
private:
	// memory pool
	CMemoryPool *m_mp;

	// array of key sets
	CColRefSetArray *m_pdrgpcrs;

	// private copy ctor
	CKeyCollection(const CKeyCollection &);

public:
	// ctors
	explicit CKeyCollection(CMemoryPool *mp);
	CKeyCollection(CMemoryPool *mp, CColRefSet *pcrs);
	CKeyCollection(CMemoryPool *mp, CColRefArray *colref_array);

	// dtor
	virtual ~CKeyCollection();

	// add individual set -- takes ownership
	void Add(CColRefSet *pcrs);

	// check if set forms a key
	BOOL FKey(const CColRefSet *pcrs, BOOL fExactMatch = true) const;

	// check if an array of columns constitutes a key
	BOOL FKey(CMemoryPool *mp, const CColRefArray *colref_array) const;

	// trim off non-key columns
	CColRefArray *PdrgpcrTrim(CMemoryPool *mp,
							  const CColRefArray *colref_array) const;

	// extract a key
	CColRefArray *PdrgpcrKey(CMemoryPool *mp) const;

	// extract a hashable key
	CColRefArray *PdrgpcrHashableKey(CMemoryPool *mp) const;

	// extract key at given position
	CColRefArray *PdrgpcrKey(CMemoryPool *mp, ULONG ul) const;

	// extract key at given position
	CColRefSet *PcrsKey(CMemoryPool *mp, ULONG ul) const;

	// number of keys
	ULONG
	Keys() const
	{
		return m_pdrgpcrs->Size();
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CKeyCollection

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CKeyCollection &kc)
{
	return kc.OsPrint(os);
}

}  // namespace gpopt

#endif
