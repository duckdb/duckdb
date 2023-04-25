//---------------------------------------------------------------------------
//	@filename:
//		CWorker.h
//
//	@doc:
//		Abstraction of schedule-able unit, e.g. a pthread etc.
//---------------------------------------------------------------------------
#ifndef GPOS_CWorker_H
#define GPOS_CWorker_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/common/CStackDescriptor.h"
#include "duckdb/optimizer/cascade/common/CTimerUser.h"
#include "duckdb/optimizer/cascade/task/CTask.h"
#include "duckdb/optimizer/cascade/task/IWorker.h"

namespace gpos
{
class CTask;

//---------------------------------------------------------------------------
//	@class:
//		CWorker
//
//	@doc:
//		Worker abstraction keeps track of resource held by worker; management
//		of control flow such as abort signal etc.
//
//---------------------------------------------------------------------------

class CWorker : public IWorker
{
	friend class CAutoTaskProxy;

private:
	// current task
	CTask *m_task;

	// available stack
	ULONG m_stack_size;

	// start address of current thread's stack
	const ULONG_PTR m_stack_start;

	// execute single task
	void Execute(CTask *task);

	// check for abort request
	void CheckForAbort(const CHAR *file, ULONG line_num);

#ifdef GPOS_FPSIMULATOR
	// simulate abort request, log abort injection
	void SimulateAbort(const CHAR *file, ULONG line_num);
#endif	// GPOS_FPSIMULATOR

	// no copy ctor
	CWorker(const CWorker &);

public:
	// ctor
	CWorker(ULONG stack_size, ULONG_PTR stack_start);

	// dtor
	virtual ~CWorker();

	// stack start accessor
	inline ULONG_PTR
	GetStackStart() const
	{
		return m_stack_start;
	}

	// stack check
	BOOL CheckStackSize(ULONG request = 0) const;

	// accessor
	inline CTask *
	GetTask()
	{
		return m_task;
	}

	// slink for hashtable
	SLink m_link;

	// lookup worker in worker pool manager
	static CWorker *
	Self()
	{
		return dynamic_cast<CWorker *>(IWorker::Self());
	}

	// host system callback function to report abort requests
	static bool (*abort_requested_by_system)(void);

};	// class CWorker
}  // namespace gpos

#endif
