//---------------------------------------------------------------------------
//	@filename:
//		CHashMapIter.h
//
//	@doc:
//		Hash map iterator
//---------------------------------------------------------------------------
#ifndef GPOS_CHashMapIter_H
#define GPOS_CHashMapIter_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CHashMap.h"
#include "duckdb/optimizer/cascade/common/CStackObject.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CHashMapIter
//
//	@doc:
//		Hash map iterator
//
//---------------------------------------------------------------------------
template <class K, class T, ULONG (*HashFn)(const K *),
		  BOOL (*EqFn)(const K *, const K *), void (*DestroyKFn)(K *),
		  void (*DestroyTFn)(T *)>
class CHashMapIter : public CStackObject
{
	// short hand for hashmap type
	typedef CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn> TMap;

private:
	// map to iterate
	const TMap *m_map;

	// current hashchain
	ULONG m_chain_idx;

	// current key
	ULONG m_key_idx;

	// is initialized?
	BOOL m_is_initialized;

	// private copy ctor
	CHashMapIter(
		const CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn> &);

	// method to return the current element
	const typename TMap::CHashMapElem *
	Get() const
	{
		typename TMap::CHashMapElem *elem = NULL;
		K *k = (*(m_map->m_keys))[m_key_idx - 1];
		elem = m_map->Lookup(k);

		return elem;
	}

public:
	// ctor
	CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>(TMap *ptm)
		: m_map(ptm), m_chain_idx(0), m_key_idx(0)
	{
		GPOS_ASSERT(NULL != ptm);
	}

	// dtor
	virtual ~CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>()
	{
	}

	// advance iterator to next element
	BOOL
	Advance()
	{
		if (m_key_idx < m_map->m_keys->Size())
		{
			m_key_idx++;
			return true;
		}

		return false;
	}

	// current key
	const K *
	Key() const
	{
		const typename TMap::CHashMapElem *elem = Get();
		if (NULL != elem)
		{
			return elem->Key();
		}
		return NULL;
	}

	// current value
	const T *
	Value() const
	{
		const typename TMap::CHashMapElem *elem = Get();
		if (NULL != elem)
		{
			return elem->Value();
		}
		return NULL;
	}

};	// class CHashMapIter

}  // namespace gpos

#endif
