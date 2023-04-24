//---------------------------------------------------------------------------
//	@filename:
//		CMemoryPoolStatistics.h
//
//	@doc:
//		Statistics for memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolStatistics_H
#define GPOS_CMemoryPoolStatistics_H

#include "duckdb/optimizer/cascade/types.h"

namespace gpos
{
// Statistics for a memory pool
class CMemoryPoolStatistics
{
private:
	ULLONG m_num_successful_allocations;

	ULLONG m_num_failed_allocations;

	ULLONG m_num_free;

	ULLONG m_num_live_obj;

	ULLONG m_live_obj_user_size;

	ULLONG m_live_obj_total_size;

	// private copy ctor
	CMemoryPoolStatistics(CMemoryPoolStatistics &);

public:
	// ctor
	CMemoryPoolStatistics()
		: m_num_successful_allocations(0),
		  m_num_failed_allocations(0),
		  m_num_free(0),
		  m_num_live_obj(0),
		  m_live_obj_user_size(0),
		  m_live_obj_total_size(0)
	{
	}

	// dtor
	virtual ~CMemoryPoolStatistics()
	{
	}

	// get the total number of successful allocation calls
	ULLONG
	GetNumSuccessfulAllocations() const
	{
		return m_num_successful_allocations;
	}

	// get the total number of failed allocation calls
	ULLONG
	GetNumFailedAllocations() const
	{
		return m_num_failed_allocations;
	}

	// get the total number of free calls
	ULLONG
	GetNumFree() const
	{
		return m_num_free;
	}

	// get the number of live objects
	ULLONG
	GetNumLiveObj() const
	{
		return m_num_live_obj;
	}

	// get the user data size of live objects
	ULLONG
	LiveObjUserSize() const
	{
		return m_live_obj_user_size;
	}

	// get the total data size (user + header padding) of live objects;
	// not accounting for memory used by the underlying allocator for its header;
	ULLONG
	LiveObjTotalSize() const
	{
		return m_live_obj_total_size;
	}

	// record a successful allocation
	void
	RecordAllocation(ULONG user_data_size, ULONG total_data_size)
	{
		++m_num_successful_allocations;
		++m_num_live_obj;
		m_live_obj_user_size += user_data_size;
		m_live_obj_total_size += total_data_size;
	}

	// record a successful free call (of a valid, non-NULL pointer)
	void
	RecordFree(ULONG user_data_size, ULONG total_data_size)
	{
		++m_num_free;
		--m_num_live_obj;
		m_live_obj_user_size -= user_data_size;
		m_live_obj_total_size -= total_data_size;
	}

	// record a failed allocation attempt
	void
	RecordFailedAllocation()
	{
		++m_num_failed_allocations;
	}

	// return total allocated size
	virtual ULLONG
	TotalAllocatedSize() const
	{
		return m_live_obj_total_size;
	}

};	// class CMemoryPoolStatistics
}  // namespace gpos

#endif
