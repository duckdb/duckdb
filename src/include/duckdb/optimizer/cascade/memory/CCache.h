//---------------------------------------------------------------------------
//	@filename:
//		CCache.h
//
//	@doc:
//		Definition of cache class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CCACHE_H_
#define GPOS_CCACHE_H_

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CAutoTimer.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtable.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableAccessByIter.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableAccessByKey.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableIter.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/memory/CCacheEntry.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolManager.h"
#include "duckdb/optimizer/cascade/task/CAutoSuspendAbort.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"


// setting the cache quota to 0 means unlimited
#define UNLIMITED_CACHE_QUOTA 0

// no. of hashtable buckets
#define CACHE_HT_NUM_OF_BUCKETS 1000

// eligible to delete
#define EXPECTED_REF_COUNT_FOR_DELETE 1

using namespace gpos;

namespace gpos
{
//prototype
template <class T, class K>
class CCacheAccessor;

//---------------------------------------------------------------------------
//	@class:
//		CCache
//
//	@doc:
//		Definition of cache;
//
//		Cache stores key-value pairs of cached objects. Keys are hashed
//		using a hashing function pointer pfuncHash. Key equality is determined
//		using a function pfuncEqual. The value of a cached object contains
//		object's key as member.
//
//		Cache API allows client to store, lookup, delete, and iterate over cached
//		objects.
//
//		Cache can only be accessed through the CCacheAccessor friend class.
//		The current implementation has a fixed gclock based eviction policy.
//
//---------------------------------------------------------------------------
template <class T, class K>
class CCache
{
	friend class CCacheAccessor<T, K>;

public:
	// type definition of key hashing and equality functions
	typedef ULONG (*HashFuncPtr)(const K &);
	typedef BOOL (*EqualFuncPtr)(const K &, const K &);

private:
	typedef CCacheEntry<T, K> CCacheHashTableEntry;

	// type definition of hashtable, accessor and iterator
	typedef CSyncHashtable<CCacheHashTableEntry, K> CCacheHashtable;
	typedef CSyncHashtableAccessByKey<CCacheHashTableEntry, K>
		CCacheHashtableAccessor;
	typedef CSyncHashtableIter<CCacheHashTableEntry, K> CCacheHashtableIter;
	typedef CSyncHashtableAccessByIter<CCacheHashTableEntry, K>
		CCacheHashtableIterAccessor;

	// memory pool for allocating hashtable and cache entries
	CMemoryPool *m_mp;

	// true if cache does not allow multiple objects with the same key
	BOOL m_unique;

	// total size of the cache in bytes
	ULLONG m_cache_size;

	// quota of the cache in bytes; 0 means unlimited quota
	ULLONG m_cache_quota;

	// initial value of gclock counter for new entries
	ULONG m_gclock_init_counter;

	// what percent of the cache size to evict
	float m_eviction_factor;

	// number of times cache entries were evicted
	ULLONG m_eviction_counter;

	// if the gclock hand was already advanced and therefore can serve the next entry
	BOOL m_clock_hand_advanced;

	// a pointer to key hashing function
	HashFuncPtr m_hash_func;

	// a pointer to key equality function
	EqualFuncPtr m_equal_func;

	// synchronized hash table; used to store and lookup entries
	CCacheHashtable m_hash_table;

	// the clock hand for gclock eviction policy
	CCacheHashtableIter *m_clock_hand;

	// inserts a new object
	CCacheHashTableEntry *
	InsertEntry(CCacheHashTableEntry *entry)
	{
		GPOS_ASSERT(NULL != entry);

		if (0 != m_cache_quota && m_cache_size > m_cache_quota)
		{
			EvictEntries();
		}

		// HERE BE DRAGONS
		//
		// One might think we can just inline the function call to
		// Key() into the constructor call. Unfortunately no.
		//
		// The accessor constructor takes a const reference to K. The
		// function returns a value of type K. The lifetime of that
		// return value (a temporary) ends at the end of the
		// constructor call. This leaves the reference (saved inside
		// the accessor object as m_key) dangling. For example, when we
		// call acc.Insert a few lines later, we load through the
		// dangling reference. If you are lucky -- and we often are --
		// specifically:
		//
		// 1. the stack space for the temporary may not be re-allocated
		// to other variables
		// 2. the old value remains after the end of life for the
		// temporary
		//
		// then we won't notice anything, the execution will get the
		// correct result.
		//
		// But if either of the above didn't happen, you end up getting
		// wrong results when dereferencing the dangling reference.
		//
		// There are generally two ways to solve this problem:
		// 1. Change m_key to be of the value type, instead of a const
		// ref. Eliminating the reference member means it will never
		// dangle. But it leaves an opportunity of error for our future
		// selves to instantiate this template with an
		// expensive-to-copy type as key.
		// 2. Keep m_key as a const reference; Be very cautious on
		// constructor calls; preferably have a continuous build
		// running ASAN
		//
		// We are picking option 2 here. We have two options to avoid
		// dangling the reference: copying the temporary (hopefully
		// cheaply), or extend its lifetime. In pre- C++11 we cannot
		// even say "move". Therefore, reach into my pocket of
		// wizardry:
		//
		// Extend the lifetime of temporary with a const ref
		//
		// TODO: Once we mandate C++11, force type K to be moveable
		const K &key = entry->Key();
		CCacheHashtableAccessor acc(m_hash_table, key);

		// if we allow duplicates, insertion can be directly made;
		// if we do not allow duplicates, we need to check first
		CCacheHashTableEntry *ret = entry;
		CCacheHashTableEntry *found = NULL;
		if (!m_unique || (m_unique && NULL == (found = acc.Find())))
		{
			acc.Insert(entry);
			m_cache_size += entry->Pmp()->TotalAllocatedSize();
		}
		else
		{
			ret = found;
		}

		ret->SetGClockCounter(m_gclock_init_counter);
		ret->IncRefCount();

		return ret;
	}

	// returns the first object matching the given key
	CCacheHashTableEntry *
	Get(const K key)
	{
		CCacheHashtableAccessor acc(m_hash_table, key);

		// look for the first unmarked entry matching the given key
		CCacheHashTableEntry *entry = acc.Find();
		while (NULL != entry && entry->IsMarkedForDeletion())
		{
			entry = acc.Next(entry);
		}

		if (NULL != entry)
		{
			entry->SetGClockCounter(m_gclock_init_counter);
			// increase ref count, since CCacheHashtableAccessor points to the obj
			// ref count will be decreased when CCacheHashtableAccessor will be destroyed
			entry->IncRefCount();
		}

		return entry;
	}

	// releases entry's memory if deleted
	void
	ReleaseEntry(CCacheHashTableEntry *entry)
	{
		GPOS_ASSERT(NULL != entry);

		// CacheEntry's destructor is the only place where ref count go from 1(EXPECTED_REF_COUNT_FOR_DELETE) to 0
		GPOS_ASSERT(EXPECTED_REF_COUNT_FOR_DELETE < entry->RefCount() &&
					"Releasing entry for which CCacheEntry has the ownership");

		BOOL deleted = false;

		// scope for hashtable accessor
		{
			// Extend the lifetime of temporary with a const ref
			// See comments in InsertEntry
			const K &key = entry->Key();
			CCacheHashtableAccessor acc(m_hash_table, key);
			entry->DecRefCount();

			if (EXPECTED_REF_COUNT_FOR_DELETE == entry->RefCount() &&
				entry->IsMarkedForDeletion())
			{
				// remove entry from hash table
				acc.Remove(entry);
				deleted = true;
			}
		}

		if (deleted)
		{
			// delete cache entry
			DestroyCacheEntry(entry);
		}
	}

	// returns the next entry in the hash chain with a key matching the given object
	CCacheHashTableEntry *
	Next(CCacheHashTableEntry *entry)
	{
		GPOS_ASSERT(NULL != entry);

		CCacheHashTableEntry *current = entry;
		K key = current->Key();
		CCacheHashtableAccessor acc(m_hash_table, key);

		// move forward until we find unmarked entry with the same key
		CCacheHashTableEntry *next = acc.Next(current);
		while (NULL != next && next->IsMarkedForDeletion())
		{
			next = acc.Next(next);
		}

		if (NULL != next)
		{
			next->IncRefCount();
		}
		GPOS_ASSERT_IMP(AllowsDuplicateKeys(), NULL == next);

		return next;
	}

	// Evict entries until the cache size is within the cache quota or until
	// the cache does not have any more evictable entries
	void
	EvictEntries()
	{
		GPOS_ASSERT(0 != m_cache_quota ||
					"Cannot evict from an unlimited sized cache");

		if (m_cache_size > m_cache_quota)
		{
			double to_free = static_cast<double>(
				static_cast<double>(m_cache_size) -
				static_cast<double>(m_cache_quota) * (1.0 - m_eviction_factor));
			GPOS_ASSERT(0 < to_free);

			ULLONG num_to_free = static_cast<ULLONG>(to_free);
			ULLONG total_freed = 0;

			// retryCount indicates the number of times we want to circle around the buckets.
			// depending on our previous cursor position (e.g., may be at the very last bucket)
			// we may end up circling 1 less time than the retry count
			for (ULONG retry_count = 0; retry_count < m_gclock_init_counter + 1;
				 retry_count++)
			{
				total_freed = EvictEntriesOnePass(total_freed, num_to_free);

				if (total_freed >= num_to_free)
				{
					// successfully freed up enough. The final action must have been a valid eviction
					GPOS_ASSERT(m_clock_hand_advanced);
					// no need to retry
					break;
				}

				// exhausted the iterator, so rewind it
				m_clock_hand->Rewind();
			}

			if (0 < total_freed)
			{
				++m_eviction_counter;
			}
		}
	}

	// cleans up when cache is destroyed
	void
	Cleanup()
	{
		m_hash_table.DestroyEntries(DestroyCacheEntryWithRefCountTest);
		GPOS_DELETE(m_clock_hand);
		m_clock_hand = NULL;
	}

	static void
	DestroyCacheEntryWithRefCountTest(CCacheHashTableEntry *entry)
	{
		GPOS_ASSERT(NULL != entry);

		// This assert is valid only when ccache get deleted. At that point nobody should hold a pointer to an object in CCache.
		// If ref count is not 1, then we possibly have a leak.
		GPOS_ASSERT(
			EXPECTED_REF_COUNT_FOR_DELETE == entry->RefCount() &&
			"Expected CCacheEntry's refcount to be 1 before it get deleted");

		DestroyCacheEntry(entry);
	}

	// destroy a cache entry
	static void
	DestroyCacheEntry(CCacheHashTableEntry *entry)
	{
		GPOS_ASSERT(NULL != entry);

		// destroy the object before deleting memory pool. This cover the case where object & cacheentry use same memory pool
		CMemoryPool *mp = entry->Pmp();
		GPOS_DELETE(entry);
		CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);
	}

	// evict entries by making one pass through the hash table buckets
	ULLONG
	EvictEntriesOnePass(ULLONG total_freed, ULLONG num_to_free)
	{
		while ((total_freed < num_to_free) &&
			   (m_clock_hand_advanced || m_clock_hand->Advance()))
		{
			m_clock_hand_advanced = false;
			CCacheHashTableEntry *entry = NULL;
			BOOL deleted = false;
			// Scope for CCacheHashtableIterAccessor
			{
				CCacheHashtableIterAccessor acc(*m_clock_hand);

				if (NULL != (entry = acc.Value()))
				{
					// can only remove when the clock hand points to a entry with 0 gclock counter
					if (0 == entry->GetGClockCounter())
					{
						// can only remove if no one else is using this entry.
						// for our self reference we are using CCacheHashtableIterAccessor
						// to directly access the entry. Therefore, we are not causing a
						// bump to ref counter
						if (EXPECTED_REF_COUNT_FOR_DELETE == entry->RefCount())
						{
							// remove advances iterator automatically
							acc.Remove(entry);
							deleted = true;

							// successfully removing an entry automatically advances the iterator, so don't call Advance()
							m_clock_hand_advanced = true;

							ULLONG num_freed =
								entry->Pmp()->TotalAllocatedSize();
							m_cache_size -= num_freed;
							total_freed += num_freed;
						}
					}
					else
					{
						entry->DecrementGClockCounter();
					}
				}
			}

			// now free the memory of the evicted entry
			if (deleted)
			{
				GPOS_ASSERT(NULL != entry);
				DestroyCacheEntry(entry);
			}
		}
		return total_freed;
	}

public:
	// ctor
	CCache(CMemoryPool *mp, BOOL unique, ULLONG cache_quota,
		   ULONG g_clock_init_counter, HashFuncPtr hash_func,
		   EqualFuncPtr equal_func)
		: m_mp(mp),
		  m_unique(unique),
		  m_cache_size(0),
		  m_cache_quota(cache_quota),
		  m_gclock_init_counter(g_clock_init_counter),
		  m_eviction_factor((float) 0.1),
		  m_eviction_counter(0),
		  m_clock_hand_advanced(false),
		  m_hash_func(hash_func),
		  m_equal_func(equal_func)
	{
		GPOS_ASSERT(NULL != m_mp &&
					"Cache memory pool could not be initialized");

		GPOS_ASSERT(0 != g_clock_init_counter);

		// initialize hashtable
		m_hash_table.Init(m_mp, CACHE_HT_NUM_OF_BUCKETS,
						  GPOS_OFFSET(CCacheHashTableEntry, m_link_hash),
						  GPOS_OFFSET(CCacheHashTableEntry, m_key),
						  (&CCacheHashTableEntry::m_invalid_key), m_hash_func,
						  m_equal_func);

		m_clock_hand = GPOS_NEW(mp) CCacheHashtableIter(m_hash_table);
	}

	// dtor
	~CCache()
	{
		Cleanup();
	}

	// does cache allow duplicate keys?
	BOOL
	AllowsDuplicateKeys() const
	{
		return m_unique;
	}

	// return number of cache entries
	ULONG_PTR
	Size() const
	{
		return m_hash_table.Size();
	}

	// return total allocated size in bytes
	ULLONG
	TotalAllocatedSize()
	{
		return m_cache_size;
	}

	// return memory quota of the cache
	ULLONG
	GetCacheQuota()
	{
		return m_cache_quota;
	}

	// return number of times this cache underwent eviction
	ULLONG
	GetEvictionCounter()
	{
		return m_eviction_counter;
	}

	// sets the cache quota
	void
	SetCacheQuota(ULLONG new_quota)
	{
		m_cache_quota = new_quota;

		if (0 != m_cache_quota && m_cache_size > m_cache_quota)
		{
			EvictEntries();
		}
	}

	// return eviction factor (what percentage of cache size to evict)
	float
	GetEvictionFactor()
	{
		return m_eviction_factor;
	}

};	//  CCache

// invalid key
template <class T, class K>
const K CCacheEntry<T, K>::m_invalid_key = NULL;

}  // namespace gpos

#endif
