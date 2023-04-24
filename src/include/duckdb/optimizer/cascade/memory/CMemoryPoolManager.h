//---------------------------------------------------------------------------
//	@filename:
//		CMemoryPoolManager.h
//
//	@doc:
//		Central memory pool manager;
//		provides factory method to generate memory pools;
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolManager_H
#define GPOS_CMemoryPoolManager_H

#include "duckdb/optimizer/cascade/common/CSyncHashtable.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableAccessByIter.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableAccessByKey.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableIter.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"

#define GPOS_MEMORY_POOL_HT_SIZE (1024)	 // number of hash table buckets

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CMemoryPoolManager
//
//	@doc:
//		Global instance of memory pool management; singleton;
//
//---------------------------------------------------------------------------
class CMemoryPoolManager
{
private:
	typedef CSyncHashtableAccessByKey<CMemoryPool, ULONG_PTR>
		MemoryPoolKeyAccessor;

	typedef CSyncHashtableIter<CMemoryPool, ULONG_PTR> MemoryPoolIter;

	typedef CSyncHashtableAccessByIter<CMemoryPool, ULONG_PTR>
		MemoryPoolIterAccessor;

	// memory pool in which all objects created by the manager itself
	// are allocated - must be thread-safe
	CMemoryPool *m_internal_memory_pool;

	// memory pool in which all objects created using global new operator
	// are allocated
	CMemoryPool *m_global_memory_pool;

	// are allocations using global new operator allowed?
	BOOL m_allow_global_new;

	// hash table to maintain created pools
	CSyncHashtable<CMemoryPool, ULONG_PTR> *m_ht_all_pools;

	// global instance
	static CMemoryPoolManager *m_memory_pool_mgr;

	// create new pool of given type
	virtual CMemoryPool *NewMemoryPool();

	// no copy ctor
	CMemoryPoolManager(const CMemoryPoolManager &);

	// clean-up memory pools
	void Cleanup();

	// destroy a memory pool at shutdown
	static void DestroyMemoryPoolAtShutdown(CMemoryPool *mp);

	// Set up CMemoryPoolManager's internals
	void Setup();

protected:
	// Used for debugging. Indicates what type of memory pool the manager handles .
	// EMemoryPoolTracker indicates the manager handles CTrackerMemoryPools.
	// EMemoryPoolExternal indicates the manager handles memory pools with logic outside
	// the gporca framework (e.g.: CPallocMemoryPool which is declared in GPDB)
	enum EMemoryPoolType
	{
		EMemoryPoolTracker = 0,
		EMemoryPoolExternal,
		EMemoryPoolSentinel
	};

	EMemoryPoolType m_memory_pool_type;

	// ctor
	CMemoryPoolManager(CMemoryPool *internal, EMemoryPoolType memory_pool_type);

	CMemoryPool *
	GetInternalMemoryPool()
	{
		return m_internal_memory_pool;
	}

	// Initialize global memory pool manager using given types
	template <typename ManagerType, typename PoolType>
	static GPOS_RESULT
	SetupGlobalMemoryPoolManager()
	{
		// raw allocation of memory for internal memory pools
		void *alloc_internal = gpos::clib::Malloc(sizeof(PoolType));

		// create internal memory pool
		CMemoryPool *internal = ::new (alloc_internal) PoolType();

		// instantiate manager
		GPOS_TRY
		{
			m_memory_pool_mgr = ::new ManagerType(internal, EMemoryPoolTracker);
			m_memory_pool_mgr->Setup();
		}
		GPOS_CATCH_EX(ex)
		{
			if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
			{
				gpos::clib::Free(alloc_internal);

				return GPOS_OOM;
			}
		}
		GPOS_CATCH_END;
		return GPOS_OK;
	}

public:
	// create new memory pool
	CMemoryPool *CreateMemoryPool();

	// release memory pool
	void Destroy(CMemoryPool *);

#ifdef GPOS_DEBUG
	// print internal contents of allocated memory pools
	IOstream &OsPrint(IOstream &os);

	// print memory pools whose allocated size above the given threshold
	void PrintOverSizedPools(CMemoryPool *trace, ULLONG size_threshold);
#endif	// GPOS_DEBUG

	// delete memory pools and release manager
	void Shutdown();

	// accessor of memory pool used in global new allocations
	CMemoryPool *
	GetGlobalMemoryPool()
	{
		return m_global_memory_pool;
	}

	virtual ~CMemoryPoolManager()
	{
	}

	// are allocations using global new operator allowed?
	BOOL
	IsGlobalNewAllowed() const
	{
		return m_allow_global_new;
	}

	// disable allocations using global new operator
	void
	DisableGlobalNew()
	{
		m_allow_global_new = false;
	}

	// enable allocations using global new operator
	void
	EnableGlobalNew()
	{
		m_allow_global_new = true;
	}

	// return total allocated size in bytes
	ULLONG TotalAllocatedSize();

	// free memory allocation
	virtual void DeleteImpl(void *ptr, CMemoryPool::EAllocationType eat);

	// get user requested size of allocation
	virtual ULONG UserSizeOfAlloc(const void *ptr);

	// initialize global instance
	static GPOS_RESULT Init();

	// global accessor
	static CMemoryPoolManager *
	GetMemoryPoolMgr()
	{
		return m_memory_pool_mgr;
	}

};	// class CMemoryPoolManager
}  // namespace gpos

#endif
