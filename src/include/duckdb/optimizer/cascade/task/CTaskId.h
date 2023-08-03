//---------------------------------------------------------------------------
//	@filename:
//		CTaskId.h
//
//	@doc:
//		Abstraction of task identification
//---------------------------------------------------------------------------
#ifndef GPOS_CTaskId_H
#define GPOS_CTaskId_H

#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CTaskId
//
//	@doc:
//		Identification class; uses a serial number.
//
//---------------------------------------------------------------------------
class CTaskId
{
private:
	// task id
	ULONG_PTR m_task_id;

	// atomic counter
	static ULONG_PTR m_counter;

public:
	struct CTaskIdHash
	{
		size_t operator()(const CTaskId &tid) const
		{
			return gpos::HashValue<ULONG_PTR>(&tid.m_task_id);
		}
	};

public:
	// ctor
	CTaskId() : m_task_id(m_counter++)
	{
	}

	// simple comparison
	bool Equals(const CTaskId &tid) const
	{
		return m_task_id == tid.m_task_id;
	}

	// comparison operator
	inline bool operator==(const CTaskId &tid) const
	{
		return this->Equals(tid);
	}

	// comparison function; used in hashtables
	static bool Equals(const CTaskId &tid, const CTaskId &other)
	{
		return tid == other;
	}

	// primitive hash function
	static ULONG HashValue(const CTaskId &tid)
	{
		return gpos::HashValue<ULONG_PTR>(&tid.m_task_id);
	}

	// invalid id
	static const CTaskId m_invalid_tid;
};	// class CTaskId
}  // namespace gpos
#endif