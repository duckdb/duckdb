//---------------------------------------------------------------------------
//	@filename:
//		CBitSet.h
//
//	@doc:
//		Implementation of bitset as linked list of bitvectors
//---------------------------------------------------------------------------
#ifndef GPOS_CBitSet_H
#define GPOS_CBitSet_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CBitVector.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CList.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CBitSet
//
//	@doc:
//		Linked list of CBitSetLink's
//
//---------------------------------------------------------------------------
class CBitSet : public CRefCount
{
	// bitset iter needs to access internals
	friend class CBitSetIter;

protected:
	//---------------------------------------------------------------------------
	//	@class:
	//		CBitSetLink
	//
	//	@doc:
	//		bit vector + offset + link
	//
	//---------------------------------------------------------------------------
	class CBitSetLink
	{
	private:
		// private copy ctor
		CBitSetLink(const CBitSetLink &);

		// offset
		ULONG m_offset;

		// bitvector
		CBitVector *m_vec;

	public:
		// ctor
		explicit CBitSetLink(CMemoryPool *, ULONG offset, ULONG vector_size);

		explicit CBitSetLink(CMemoryPool *, const CBitSetLink &);

		// dtor
		~CBitSetLink();

		// accessor
		ULONG
		GetOffset() const
		{
			return m_offset;
		}

		// accessor
		CBitVector *
		GetVec() const
		{
			return m_vec;
		}

		// list link
		SLink m_link;

	};	// class CBitSetLink

	// list of bit set links
	CList<CBitSetLink> m_bsllist;

	// pool to allocate links from
	CMemoryPool *m_mp;

	// size of individual bitvectors
	ULONG m_vector_size;

	// number of elements
	ULONG m_size;

	// private copy ctor
	CBitSet(const CBitSet &);

	// find link with offset less or equal to given value
	CBitSetLink *FindLinkByOffset(ULONG, CBitSetLink * = NULL) const;

	// reset set
	void Clear();

	// compute target offset
	ULONG ComputeOffset(ULONG) const;

	// re-compute size of set
	void RecomputeSize();

public:
	// ctor
	CBitSet(CMemoryPool *mp, ULONG vector_size = 256);
	CBitSet(CMemoryPool *mp, const CBitSet &);

	// dtor
	virtual ~CBitSet();

	// determine if bit is set
	BOOL Get(ULONG pos) const;

	// set given bit; return previous value
	BOOL ExchangeSet(ULONG pos);

	// clear given bit; return previous value
	BOOL ExchangeClear(ULONG pos);

	// union sets
	void Union(const CBitSet *);

	// intersect sets
	void Intersection(const CBitSet *);

	// difference of sets
	void Difference(const CBitSet *);

	// is subset
	BOOL ContainsAll(const CBitSet *) const;

	// equality
	BOOL Equals(const CBitSet *) const;

	// disjoint
	BOOL IsDisjoint(const CBitSet *) const;

	// hash value for set
	ULONG HashValue() const;

	// number of elements
	ULONG
	Size() const
	{
		return m_size;
	}

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CBitSet


// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CBitSet &bs)
{
	return bs.OsPrint(os);
}
}  // namespace gpos

#endif
