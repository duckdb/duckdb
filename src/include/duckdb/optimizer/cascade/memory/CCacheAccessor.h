//---------------------------------------------------------------------------
//	@filename:
//		CCacheAccessor.h
//
//	@doc:
//		Definition of cache accessor base class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CCACHEACCESSOR_H_
#define GPOS_CCACHEACCESSOR_H_

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/memory/CCache.h"

using namespace gpos;

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CCacheAccessor
//
//	@doc:
//		Definition of CCacheAccessor;
//
//		The accessor holds exactly one cached object at a time.
//
//		The accessor's API includes four main functions:
//			(1) Inserting a new object in the cache
//			(2) Looking up a cached object by key
//			(3) Deleting a cached object
//			(4) Iterating over cached objects with the same key
//---------------------------------------------------------------------------
template <class T, class K>
class CCacheAccessor
{
private:
	// the underlying cache
	CCache<T, K> *m_cache;

	// memory pool of a cached object inserted by the accessor
	CMemoryPool *m_mp;

	// cached object currently held by the accessor
	typename CCache<T, K>::CCacheHashTableEntry *m_entry;

	// true if insertion of a new object into the cache was successful
	BOOL m_inserted;

public:
	// ctor; protected to disable instantiation unless from child class
	CCacheAccessor(CCache<T, K> *cache)
		: m_cache(cache), m_mp(NULL), m_entry(NULL), m_inserted(false)

	{
		GPOS_ASSERT(NULL != cache);
	}

	// dtor
	~CCacheAccessor()
	{
		// check if a memory pool was created but insertion failed
		if (NULL != m_mp && !m_inserted)
		{
			CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(m_mp);
		}

		// release entry if one was created
		if (NULL != m_entry)
		{
			m_cache->ReleaseEntry(m_entry);
		}
	}

	// the following functions are hidden since they involve
	// (void *) key/value data types; the actual types are defined
	// as template parameters in the child class CCacheAccessor

	// inserts a new object into the cache
	T
	Insert(K key, T val)
	{
		GPOS_ASSERT(NULL != m_mp);

		GPOS_ASSERT(!m_inserted && "Accessor was already used for insertion");

		GPOS_ASSERT(NULL == m_entry && "Accessor already holds an entry");

		CCacheEntry<T, K> *entry = GPOS_NEW(m_cache->m_mp)
			CCacheEntry<T, K>(m_mp, key, val, m_cache->m_gclock_init_counter);

		CCacheEntry<T, K> *ret = m_cache->InsertEntry(entry);

		// check if insertion completed successfully
		if (entry == ret)
		{
			m_inserted = true;
		}
		else
		{
			GPOS_DELETE(entry);
		}

		// accessor holds the returned entry in all cases
		m_entry = ret;

		return ret->Val();
	}

	// returns the object currently held by the accessor
	T
	Val() const
	{
		T ret = NULL;
		if (NULL != m_entry)
		{
			ret = m_entry->Val();
		}

		return ret;
	}

	// gets the next undeleted object with the same key
	T
	Next()
	{
		GPOS_ASSERT(NULL != m_entry);

		typename CCache<T, K>::CCacheHashTableEntry *entry = m_entry;
		m_entry = m_cache->Next(m_entry);

		// release previous entry
		m_cache->ReleaseEntry(entry);

		return Val();
	}

public:
	// creates a new memory pool for allocating a new object
	CMemoryPool *
	Pmp()
	{
		GPOS_ASSERT(NULL == m_mp);

		// construct a memory pool for cache entry
		m_mp = CMemoryPoolManager::GetMemoryPoolMgr()->CreateMemoryPool();

		return m_mp;
	}

	// finds the first object matching the given key
	void
	Lookup(K key)
	{
		GPOS_ASSERT(NULL == m_entry && "Accessor already holds an entry");

		m_entry = m_cache->Get(key);

		if (NULL != m_entry)
		{
			// increase ref count before return the object to the customer
			// customer is responsible for decreasing the ref count after use
			m_entry->IncRefCount();
		}
	}

	// marks currently held object as deleted
	void
	MarkForDeletion()
	{
		GPOS_ASSERT(NULL != m_entry);

		m_entry->MarkForDeletion();
	}

};	// CCacheAccessor

}  //namespace gpos

#endif