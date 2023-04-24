//---------------------------------------------------------------------------
//	@filename:
//		CHashMap.h
//
//	@doc:
//		Hash map
//		* stores deep objects, i.e., pointers
//		* equality == on key uses template function argument
//		* does not allow insertion of duplicates (no equality on value class req'd)
//		* destroys objects based on client-side provided destroy functions
//---------------------------------------------------------------------------
#ifndef GPOS_CHashMap_H
#define GPOS_CHashMap_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

namespace gpos
{
// fwd declaration
template <class K, class T, ULONG (*HashFn)(const K *),
		  BOOL (*EqFn)(const K *, const K *), void (*DestroyKFn)(K *),
		  void (*DestroyTFn)(T *)>
class CHashMapIter;

//---------------------------------------------------------------------------
//	@class:
//		CHashMap
//
//	@doc:
//		Hash map
//
//---------------------------------------------------------------------------
template <class K, class T, ULONG (*HashFn)(const K *),
		  BOOL (*EqFn)(const K *, const K *), void (*DestroyKFn)(K *),
		  void (*DestroyTFn)(T *)>
class CHashMap : public CRefCount
{
	// fwd declaration
	friend class CHashMapIter<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>;

private:
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapElem
	//
	//	@doc:
	//		Anchor for key/value pair
	//
	//---------------------------------------------------------------------------
	class CHashMapElem
	{
	private:
		// key/value pair
		K *m_key;
		T *m_value;

		// own objects
		BOOL m_owns_objects;

		// private copy ctor
		CHashMapElem(const CHashMapElem &);

	public:
		// ctor
		CHashMapElem(K *key, T *value, BOOL fOwn)
			: m_key(key), m_value(value), m_owns_objects(fOwn)
		{
			GPOS_ASSERT(NULL != key);
		}

		// dtor
		~CHashMapElem()
		{
			// in case of a temporary hashmap element for lookup we do NOT own the
			// objects, otherwise call destroy functions
			if (m_owns_objects)
			{
				DestroyKFn(m_key);
				DestroyTFn(m_value);
			}
		}

		// key accessor
		K *
		Key() const
		{
			return m_key;
		}

		// value accessor
		T *
		Value() const
		{
			return m_value;
		}

		// replace value
		void
		ReplaceValue(T *new_value)
		{
			if (m_owns_objects)
			{
				DestroyTFn(m_value);
			}
			m_value = new_value;
		}

		// equality operator -- map elements are equal if their keys match
		BOOL
		operator==(const CHashMapElem &elem) const
		{
			return EqFn(m_key, elem.m_key);
		}
	};

	// memory pool
	CMemoryPool *const m_mp;

	// size
	ULONG m_num_chains;

	// number of entries
	ULONG m_size;

	// each hash chain is an array of hashmap elements
	typedef CDynamicPtrArray<CHashMapElem, CleanupDelete> CHashSetElemArray;
	CHashSetElemArray **const m_chains;

	// array for keys
	// We use CleanupNULL because the keys are owned by the hash table
	typedef CDynamicPtrArray<K, CleanupNULL> Keys;
	Keys *const m_keys;

	IntPtrArray *const m_filled_chains;

	// private copy ctor
	CHashMap(const CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn> &);

	// lookup appropriate hash chain in static table, may be NULL if
	// no elements have been inserted yet
	CHashSetElemArray **
	GetChain(const K *key) const
	{
		GPOS_ASSERT(NULL != m_chains);
		return &m_chains[HashFn(key) % m_num_chains];
	}

	// clear elements
	void
	Clear()
	{
		for (ULONG i = 0; i < m_filled_chains->Size(); i++)
		{
			// release each hash chain
			m_chains[*(*m_filled_chains)[i]]->Release();
		}
		m_size = 0;
		m_filled_chains->Clear();
	}

	// lookup an element by its key
	CHashMapElem *
	Lookup(const K *key) const
	{
		CHashMapElem hme(const_cast<K *>(key), NULL /*T*/, false /*fOwn*/);
		CHashMapElem *found_hme = NULL;
		CHashSetElemArray **chain = GetChain(key);
		if (NULL != *chain)
		{
			found_hme = (*chain)->Find(&hme);
			GPOS_ASSERT_IMP(NULL != found_hme, *found_hme == hme);
		}

		return found_hme;
	}

public:
	// ctor
	CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>(CMemoryPool *mp,
														 ULONG num_chains = 127)
		: m_mp(mp),
		  m_num_chains(num_chains),
		  m_size(0),
		  m_chains(GPOS_NEW_ARRAY(m_mp, CHashSetElemArray *, m_num_chains)),
		  m_keys(GPOS_NEW(m_mp) Keys(m_mp)),
		  m_filled_chains(GPOS_NEW(mp) IntPtrArray(mp))
	{
		GPOS_ASSERT(m_num_chains > 0);
		(void) clib::Memset(m_chains, 0,
							m_num_chains * sizeof(CHashSetElemArray *));
	}

	// dtor
	~CHashMap<K, T, HashFn, EqFn, DestroyKFn, DestroyTFn>()
	{
		// release all hash chains
		Clear();

		GPOS_DELETE_ARRAY(m_chains);
		m_keys->Release();
		m_filled_chains->Release();
	}

	// insert an element if key is not yet present
	BOOL
	Insert(K *key, T *value)
	{
		if (NULL != Find(key))
		{
			return false;
		}

		CHashSetElemArray **chain = GetChain(key);
		if (NULL == *chain)
		{
			*chain = GPOS_NEW(m_mp) CHashSetElemArray(m_mp);
			INT chain_idx = HashFn(key) % m_num_chains;
			m_filled_chains->Append(GPOS_NEW(m_mp) INT(chain_idx));
		}

		CHashMapElem *elem =
			GPOS_NEW(m_mp) CHashMapElem(key, value, true /*fOwn*/);
		(*chain)->Append(elem);

		m_size++;
		m_keys->Append(key);

		return true;
	}

	// lookup a value by its key
	T *
	Find(const K *key) const
	{
		CHashMapElem *elem = Lookup(key);
		if (NULL != elem)
		{
			return elem->Value();
		}

		return NULL;
	}

	// replace the value in a map entry with a new given value
	BOOL
	Replace(const K *key, T *ptNew)
	{
		GPOS_ASSERT(NULL != key);

		BOOL fSuccess = false;
		CHashMapElem *elem = Lookup(key);
		if (NULL != elem)
		{
			elem->ReplaceValue(ptNew);
			fSuccess = true;
		}

		return fSuccess;
	}

	BOOL
	Delete(const K *key)
	{
		CHashSetElemArray **chain = GetChain(key);

		if (NULL != *chain)
		{
			for (ULONG ul = 0; ul < (*chain)->Size(); ul++)
			{
				if (EqFn((**chain)[ul]->Key(), key))
				{
					// found the entry, now remove it by putting it last,
					// then removing the last element
					(*chain)->Swap(ul, (*chain)->Size() - 1);
					CHashMapElem *to_delete = (*chain)->RemoveLast();
					CleanupDelete(to_delete);
					return true;
				}
			}
		}
		return false;
	}

	// return number of map entries
	ULONG
	Size() const
	{
		return m_size;
	}

};	// class CHashMap

}  // namespace gpos

#endif
