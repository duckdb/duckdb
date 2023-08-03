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
#include <list>

using namespace std;

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
	list<CTask*> m_task_queue;

public:
	// ctor
	CTaskSchedulerFifo()
	{
	}
	
	// private copy ctor
	CTaskSchedulerFifo(const CTaskSchedulerFifo &) = delete;
	
	// dtor
	~CTaskSchedulerFifo()
	{
	}

	// add task to waiting queue
	void Enqueue(CTask* task) override;

	// get next task to execute
	CTask* Dequeue() override;

	// check if task is waiting to be scheduled and remove it
	GPOS_RESULT Cancel(CTask* task) override;

	// get number of waiting tasks
	ULONG GetQueueSize() override
	{
		return m_task_queue.size();
	}

	// check if task queue is empty
	bool IsEmpty() const override
	{
		return m_task_queue.empty();
	}
};	// class CTaskSchedulerFifo
}  // namespace gpos
#endif