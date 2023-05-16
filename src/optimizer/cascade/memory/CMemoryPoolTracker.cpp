//---------------------------------------------------------------------------
//	@filename:
//		CMemoryPoolTracker.cpp
//
//	@doc:
//		Implementation for memory pool that allocates from Malloc
//		and adds synchronization, statistics, debugging information
//		and memory tracing.
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/memory/CMemoryPoolTracker.h"
#include "duckdb/optimizer/cascade/assert.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolManager.h"
#include "duckdb/optimizer/cascade/memory/IMemoryVisitor.h"
#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

using namespace gpos;

#define GPOS_MEM_GUARD_SIZE (GPOS_SIZEOF(BYTE))

#define GPOS_MEM_ALLOC_HEADER_SIZE GPOS_MEM_ALIGNED_STRUCT_SIZE(SAllocHeader)

#define GPOS_MEM_BYTES_TOTAL(ulNumBytes) \
	(GPOS_MEM_ALLOC_HEADER_SIZE +        \
	 GPOS_MEM_ALIGNED_SIZE((ulNumBytes) + GPOS_MEM_GUARD_SIZE))


// ctor
CMemoryPoolTracker::CMemoryPoolTracker() : CMemoryPool(), m_alloc_sequence(0)
{
	m_allocations_list.Init(GPOS_OFFSET(SAllocHeader, m_link));
}


// dtor
CMemoryPoolTracker::~CMemoryPoolTracker()
{
	GPOS_ASSERT(m_allocations_list.IsEmpty());
}

void
CMemoryPoolTracker::RecordAllocation(SAllocHeader *header)
{
	m_memory_pool_statistics.RecordAllocation(header->m_user_size,
											  header->m_alloc_size);
	m_allocations_list.Prepend(header);
}

void
CMemoryPoolTracker::RecordFree(SAllocHeader *header)
{
	m_memory_pool_statistics.RecordFree(header->m_user_size,
										header->m_alloc_size);
	m_allocations_list.Remove(header);
}


void *
CMemoryPoolTracker::NewImpl(const ULONG bytes, const CHAR *file,
							const ULONG line, CMemoryPool::EAllocationType eat)
{
	GPOS_ASSERT(bytes <= GPOS_MEM_ALLOC_MAX);
	GPOS_ASSERT(bytes <= gpos::ulong_max);
	GPOS_ASSERT_IMP(
		(NULL != CMemoryPoolManager::GetMemoryPoolMgr()) &&
			(this ==
			 CMemoryPoolManager::GetMemoryPoolMgr()->GetGlobalMemoryPool()),
		CMemoryPoolManager::GetMemoryPoolMgr()->IsGlobalNewAllowed() &&
			"Use of new operator without target memory pool is prohibited, use New(...) instead");

	ULONG alloc_size = GPOS_MEM_BYTES_TOTAL(bytes);

	void *ptr = clib::Malloc(alloc_size);

	// check if allocation failed
	if (NULL == ptr)
	{
		return NULL;
	}

	GPOS_OOM_CHECK(ptr);

	// successful allocation: update header information and any memory pool data
	SAllocHeader *header = static_cast<SAllocHeader *>(ptr);
	header->m_serial = m_alloc_sequence;
	++m_alloc_sequence;

	header->m_alloc_size = alloc_size;
	header->m_mp = this;
	header->m_filename = file;
	header->m_line = line;
	header->m_user_size = bytes;

	RecordAllocation(header);

	void *ptr_result = header + 1;

#ifdef GPOS_DEBUG
	header->m_stack_desc.BackTrace();

	clib::Memset(ptr_result, GPOS_MEM_INIT_PATTERN_CHAR, bytes);
#endif	// GPOS_DEBUG

	// add a footer with the allocation type (singleton/array)
	BYTE *alloc_type = reinterpret_cast<BYTE *>(ptr_result) + bytes;
	*alloc_type = eat;

	return ptr_result;
}

// free memory allocation
void
CMemoryPoolTracker::DeleteImpl(void *ptr, EAllocationType eat)
{
	SAllocHeader *header = static_cast<SAllocHeader *>(ptr) - 1;

	ULONG user_size = header->m_user_size;
	BYTE *alloc_type = static_cast<BYTE *>(ptr) + user_size;

	// this assert ensures we aren't writing past allocated memory
	GPOS_RTL_ASSERT(eat == EatUnknown || *alloc_type == eat);

	// update stats and allocation list
	GPOS_ASSERT(NULL != header->m_mp);
	header->m_mp->RecordFree(header);

#ifdef GPOS_DEBUG
	// mark user memory as unused in debug mode
	clib::Memset(ptr, GPOS_MEM_FREED_PATTERN_CHAR, user_size);
#endif	// GPOS_DEBUG

	clib::Free(header);
}

// get user requested size of allocation
ULONG
CMemoryPoolTracker::UserSizeOfAlloc(const void *ptr)
{
	const SAllocHeader *header = static_cast<const SAllocHeader *>(ptr) - 1;
	return header->m_user_size;
}


// Prepare the memory pool to be deleted;
// this function is called only once so locking is not required;
void
CMemoryPoolTracker::TearDown()
{
	while (!m_allocations_list.IsEmpty())
	{
		SAllocHeader *header = m_allocations_list.First();
		void *user_data = header + 1;
		DeleteImpl(user_data, EatUnknown);
	}
}


#ifdef GPOS_DEBUG

void
CMemoryPoolTracker::WalkLiveObjects(gpos::IMemoryVisitor *visitor)
{
	GPOS_ASSERT(NULL != visitor);

	SAllocHeader *header = m_allocations_list.First();
	while (NULL != header)
	{
		void *user = header + 1;

		visitor->Visit(user, header->m_user_size, header, header->m_alloc_size,
					   header->m_filename, header->m_line, header->m_serial,
#ifdef GPOS_DEBUG
					   &header->m_stack_desc
#else
					   NULL
#endif	// GPOS_DEBUG
		);

		header = m_allocations_list.Next(header);
	}
}

#endif	// GPOS_DEBUG