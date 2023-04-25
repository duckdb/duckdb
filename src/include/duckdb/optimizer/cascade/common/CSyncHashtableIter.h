//---------------------------------------------------------------------------
//	@filename:
//		CSyncHashtableIter.h
//
//	@doc:
//		Iterator for allocation-less static hashtable; this class encapsulates
//		the state of iteration process (the current bucket we iterate through
//		and iterator position); it also allows advancing iterator's position
//		in the hash table; accessing the element at current iterator's
//		position is provided by the iterator's accessor class.
//
//		Since hash table iteration/insertion/deletion can happen concurrently,
//		the semantics of iteration output are defined as follows:
//			- the iterator sees all elements that have not been removed; if
//			any of them are removed concurrently, we may or may not see them
//			- New elements may or may not be seen
//			- No element is returned twice
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtableIter_H
#define GPOS_CSyncHashtableIter_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtable.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableAccessByIter.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CSyncHashtableIter<T, K, S>
//
//	@doc:
//		Iterator class has to know all template parameters of
//		the hashtable class in order to link to a target hashtable.
//
//---------------------------------------------------------------------------
template <class T, class K>
class CSyncHashtableIter
{
	// iterator accessor class is a friend
	friend class CSyncHashtableAccessByIter<T, K>;

private:
	// target hashtable
	CSyncHashtable<T, K> &m_ht;

	// index of bucket to operate on
	ULONG m_bucket_idx;

	// a slab of memory to manufacture an invalid element; we enforce
	// memory alignment here to mimic allocation of a class object
	ALIGN_STORAGE BYTE m_invalid_elem_data[sizeof(T)];

	// a pointer to memory slab to interpret it as invalid element
	T *m_invalid_elem;

	// no copy ctor
	CSyncHashtableIter<T, K>(const CSyncHashtableIter<T, K> &);

	// inserts invalid element at the head of current bucket
	void
	InsertInvalidElement()
	{
		m_invalid_elem_inserted = false;
		while (m_bucket_idx < m_ht.m_nbuckets)
		{
			CSyncHashtableAccessByIter<T, K> acc(*this);

			T *first = acc.First();
			T *first_valid = NULL;

			if (NULL != first &&
				(NULL != (first_valid = acc.FirstValid(first))))
			{
				// insert invalid element before the found element
				acc.Prepend(m_invalid_elem, first_valid);
				m_invalid_elem_inserted = true;
				break;
			}
			else
			{
				m_bucket_idx++;
			}
		}
	}

	// advances invalid element in current bucket
	void
	AdvanceInvalidElement()
	{
		CSyncHashtableAccessByIter<T, K> acc(*this);

		T *value = acc.FirstValid(m_invalid_elem);

		acc.Remove(m_invalid_elem);
		m_invalid_elem_inserted = false;

		// check that we did not find the last element in bucket
		if (NULL != value && NULL != acc.Next(value))
		{
			// insert invalid element after the found element
			acc.Append(m_invalid_elem, value);
			m_invalid_elem_inserted = true;
		}
	}

	// a flag indicating if invalid element is currently in the hash table
	BOOL m_invalid_elem_inserted;

public:
	// ctor
	explicit CSyncHashtableIter<T, K>(CSyncHashtable<T, K> &ht)
		: m_ht(ht),
		  m_bucket_idx(0),
		  m_invalid_elem(NULL),
		  m_invalid_elem_inserted(false)
	{
		m_invalid_elem = (T *) m_invalid_elem_data;

		// get a reference to invalid element's key
		K &key = m_ht.Key(m_invalid_elem);

		// copy invalid key to invalid element's memory
		(void) clib::Memcpy(&key, m_ht.m_invalid_key,
							sizeof(*(m_ht.m_invalid_key)));
	}

	// dtor
	~CSyncHashtableIter<T, K>()
	{
		if (m_invalid_elem_inserted)
		{
			// remove invalid element
			CSyncHashtableAccessByIter<T, K> acc(*this);
			acc.Remove(m_invalid_elem);
		}
	}

	// advances iterator
	BOOL
	Advance()
	{
		GPOS_ASSERT(m_bucket_idx < m_ht.m_nbuckets &&
					"Advancing an exhausted iterator");

		if (!m_invalid_elem_inserted)
		{
			// first time to call iterator's advance, insert invalid
			// element into the first non-empty bucket
			InsertInvalidElement();
		}
		else
		{
			AdvanceInvalidElement();

			if (!m_invalid_elem_inserted)
			{
				// current bucket is exhausted, insert invalid element
				// in the next unvisited non-empty bucket
				m_bucket_idx++;
				InsertInvalidElement();
			}
		}

		return (m_bucket_idx < m_ht.m_nbuckets);
	}

	// rewinds the iterator to the beginning
	void
	Rewind()
	{
		GPOS_ASSERT(m_bucket_idx >= m_ht.m_nbuckets &&
					"Rewinding an un-exhausted iterator");
		GPOS_ASSERT(
			!m_invalid_elem_inserted &&
			"Invalid element from previous iteration exists, cannot rewind");

		m_bucket_idx = 0;
	}

};	// class CSyncHashtableIter

}  // namespace gpos

#endif
