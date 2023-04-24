//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc
//
//	Hash set iterator

#ifndef GPOS_CHashSetIter_H
#define GPOS_CHashSetIter_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CHashSet.h"
#include "duckdb/optimizer/cascade/common/CStackObject.h"

namespace gpos
{
// Hash set iterator
template <class T, ULONG (*HashFn)(const T *),
		  BOOL (*EqFn)(const T *, const T *), void (*CleanupFn)(T *)>
class CHashSetIter : public CStackObject
{
	// short hand for hashset type
	typedef CHashSet<T, HashFn, EqFn, CleanupFn> TSet;

private:
	// set to iterate
	const TSet *m_set;

	// current hashchain
	ULONG m_chain_idx;

	// current element
	ULONG m_elem_idx;

	// is initialized?
	BOOL m_is_initialized;

	// private copy ctor
	CHashSetIter(const CHashSetIter<T, HashFn, EqFn, CleanupFn> &);

public:
	// ctor
	CHashSetIter<T, HashFn, EqFn, CleanupFn>(TSet *set)
		: m_set(set), m_chain_idx(0), m_elem_idx(0)
	{
		GPOS_ASSERT(NULL != set);
	}

	// dtor
	virtual ~CHashSetIter<T, HashFn, EqFn, CleanupFn>()
	{
	}

	// advance iterator to next element
	BOOL
	Advance()
	{
		if (m_elem_idx < m_set->m_elements->Size())
		{
			m_elem_idx++;
			return true;
		}

		return false;
	}

	// current element
	const T *
	Get() const
	{
		const typename TSet::CHashSetElem *elem = NULL;
		T *t = (*(m_set->m_elements))[m_elem_idx - 1];
		elem = m_set->Lookup(t);
		if (NULL != elem)
		{
			return elem->Value();
		}
		return NULL;
	}

};	// class CHashSetIter

}  // namespace gpos

#endif
