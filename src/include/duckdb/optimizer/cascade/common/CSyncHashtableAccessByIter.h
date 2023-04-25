//---------------------------------------------------------------------------
//	@filename:
//		CSyncHashtableAccessByIter.h
//
//	@doc:
//		Iterator's accessor provides protected access to hashtable elements
//		during iteration.
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtableAccessByIter_H
#define GPOS_CSyncHashtableAccessByIter_H

#include "duckdb/optimizer/cascade/common/CSyncHashtableAccessorBase.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableIter.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CSyncHashtableAccessByIter<T, K, S>
//
//	@doc:
//		Accessor class to provide access to the element pointed to by a
//		hash table iterator
//
//---------------------------------------------------------------------------
template <class T, class K>
class CSyncHashtableAccessByIter : public CSyncHashtableAccessorBase<T, K>
{
	// iterator class is a friend
	friend class CSyncHashtableIter<T, K>;

private:
	// shorthand for base class
	typedef class CSyncHashtableAccessorBase<T, K> Base;

	// target iterator
	CSyncHashtableIter<T, K> &m_iter;

	// no copy ctor
	CSyncHashtableAccessByIter<T, K>(const CSyncHashtableAccessByIter<T, K> &);

	// returns the first valid element starting from the given element
	T *
	FirstValid(T *value) const
	{
		GPOS_ASSERT(NULL != value);

		T *curr = value;
		while (NULL != curr &&
			   !Base::GetHashTable().IsValid(Base::GetHashTable().Key(curr)))
		{
			curr = Base::Next(curr);
		}

		return curr;
	}

public:
	// ctor
	explicit CSyncHashtableAccessByIter<T, K>(CSyncHashtableIter<T, K> &iter)
		: Base(iter.m_ht, iter.m_bucket_idx), m_iter(iter)
	{
	}

	// returns the element pointed to by iterator
	T *
	Value() const
	{
		GPOS_ASSERT(m_iter.m_invalid_elem_inserted &&
					"Iterator's advance is not called");

		// advance in the current bucket until finding a valid element;
		// this is needed because the next valid element pointed to by
		// iterator might have been deleted by another client just before
		// using the accessor

		return FirstValid(m_iter.m_invalid_elem);
	}

};	// class CSyncHashtableAccessByIter

}  // namespace gpos

#endif
