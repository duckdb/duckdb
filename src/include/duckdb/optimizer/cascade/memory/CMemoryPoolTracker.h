//---------------------------------------------------------------------------
//	@filename:
//		CMemoryPoolTracker.h
//
//	@doc:
//		Memory pool that allocates from malloc() and adds on
//		statistics and debugging
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolTracker_H
#define GPOS_CMemoryPoolTracker_H

#include "duckdb/optimizer/cascade/assert.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/common/CStackDescriptor.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"
#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpos
{
// memory pool with statistics and debugging support
class CMemoryPoolTracker : public CMemoryPool
{
private:
	// Defines memory block header layout for all allocations;
	// does not include the pointer to the pool;
	struct SAllocHeader
	{
		// pointer to pool
		CMemoryPoolTracker *m_mp;

		// total allocation size (including headers)
		ULONG m_alloc_size;

		// user requested size
		ULONG m_user_size;

		// sequence number
		ULLONG m_serial;

		// file name
		const CHAR *m_filename;

		// line in file
		ULONG m_line;

#ifdef GPOS_DEBUG
		// allocation stack
		CStackDescriptor m_stack_desc;
#endif	// GPOS_DEBUG

		// link for allocation list
		SLink m_link;
	};

	// statistics
	CMemoryPoolStatistics m_memory_pool_statistics;

	// allocation sequence number
	ULONG m_alloc_sequence;

	// list of allocated (live) objects
	CList<SAllocHeader> m_allocations_list;

	// private copy ctor
	CMemoryPoolTracker(CMemoryPoolTracker &);

	// record a successful allocation
	void RecordAllocation(SAllocHeader *header);

	// record a successful free
	void RecordFree(SAllocHeader *header);

protected:
	// dtor
	virtual ~CMemoryPoolTracker();

public:
	// ctor
	CMemoryPoolTracker();

	// prepare the memory pool to be deleted
	virtual void TearDown();

	// allocate memory
	void *NewImpl(const ULONG bytes, const CHAR *file, const ULONG line,
				  CMemoryPool::EAllocationType eat);

	// free memory allocation
	static void DeleteImpl(void *ptr, EAllocationType eat);

	// get user requested size of allocation
	static ULONG UserSizeOfAlloc(const void *ptr);

	// return total allocated size
	virtual ULLONG
	TotalAllocatedSize() const
	{
		return m_memory_pool_statistics.TotalAllocatedSize();
	}

#ifdef GPOS_DEBUG

	// check if the memory pool keeps track of live objects
	virtual BOOL
	SupportsLiveObjectWalk() const
	{
		return true;
	}

	// walk the live objects
	virtual void WalkLiveObjects(gpos::IMemoryVisitor *visitor);

#endif	// GPOS_DEBUG
};
}  // namespace gpos

#endif