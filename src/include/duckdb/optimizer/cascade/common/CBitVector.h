//---------------------------------------------------------------------------
//	@filename:
//		CBitVector.h
//
//	@doc:
//		Implementation of static bit vector;
//---------------------------------------------------------------------------
#ifndef GPOS_CBitVector_H
#define GPOS_CBitVector_H

#include "duckdb/optimizer/cascade/base.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CBitVector
//
//	@doc:
//		Bit vector based on ULLONG elements
//
//---------------------------------------------------------------------------
class CBitVector
{
private:
	// size in bits
	ULONG m_nbits;

	// size of vector in units, not bits
	ULONG m_len;

	// vector
	ULLONG *m_vec;

	// no default copy ctor
	CBitVector(const CBitVector &);

	// clear vector
	void Clear();

public:
	// ctor
	CBitVector(CMemoryPool *mp, ULONG cBits);

	// dtor
	~CBitVector();

	// copy ctor with target mem pool
	CBitVector(CMemoryPool *mp, const CBitVector &);

	// determine if bit is set
	BOOL Get(ULONG ulBit) const;

	// set given bit; return previous value
	BOOL ExchangeSet(ULONG ulBit);

	// clear given bit; return previous value
	BOOL ExchangeClear(ULONG ulBit);

	// union vectors
	void Or(const CBitVector *);

	// intersect vectors
	void And(const CBitVector *);

	// is subset
	BOOL ContainsAll(const CBitVector *) const;

	// is dijoint
	BOOL IsDisjoint(const CBitVector *) const;

	// equality
	BOOL Equals(const CBitVector *) const;

	// is empty?
	BOOL IsEmpty() const;

	// find next bit from given position
	BOOL GetNextSetBit(ULONG, ULONG &) const;

	// number of bits set
	ULONG CountSetBits() const;

	// hash value
	ULONG HashValue() const;

};	// class CBitVector

}  // namespace gpos

#endif
