//---------------------------------------------------------------------------
//	@filename:
//		CTaskSchedulerFifo.h
//
//	@doc:
//		Task scheduler using FIFO.
//---------------------------------------------------------------------------
#ifndef GPOS_CTaskSchedulerFifo_H
#define GPOS_CTaskSchedulerFifo_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/task/CTask.h"
#include "duckdb/optimizer/cascade/task/ITaskScheduler.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CTaskScheduler
//
//	@doc:
//		Task scheduler abstraction maintains collection of tasks waiting to
//		execute and decides which task will execute next. The scheduling
//		algorithm is FIFO, i.e. tasks are queued and scheduled in the order
//		they arrive to the scheduler.
//
//		Queue operations are not thread-safe. The caller (worker pool
//		manager) is responsible for synchronizing concurrent accesses to
//		task scheduler.
//
//---------------------------------------------------------------------------

class CTaskSchedulerFifo : public ITaskScheduler
{
private:
	// task queue
	CList<CTask> m_task_queue;

	// private copy ctor
	CTaskSchedulerFifo(const CTaskSchedulerFifo &);

public:
	// ctor
	CTaskSchedulerFifo()
	{
		m_task_queue.Init(GPOS_OFFSET(CTask, m_task_scheduler_link));
	}

	// dtor
	~CTaskSchedulerFifo()
	{
	}

	// add task to waiting queue
	void Enqueue(CTask *task);

	// get next task to execute
	CTask *Dequeue();

	// check if task is waiting to be scheduled and remove it
	GPOS_RESULT Cancel(CTask *task);

	// get number of waiting tasks
	ULONG
	GetQueueSize()
	{
		return m_task_queue.Size();
	}

	// check if task queue is empty
	BOOL
	IsEmpty() const
	{
		return m_task_queue.IsEmpty();
	}

};	// class CTaskSchedulerFifo
}  // namespace gpos

#endif