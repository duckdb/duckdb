//---------------------------------------------------------------------------
//	@filename:
//		CTaskSchedulerFifo.cpp
//
//	@doc:
//		Implementation of task scheduler with FIFO.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/task/CTaskSchedulerFifo.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CTaskSchedulerFifo::Enqueue
//
//	@doc:
//		Add task to waiting queue
//
//---------------------------------------------------------------------------
void
CTaskSchedulerFifo::Enqueue(CTask *task)
{
	m_task_queue.Append(task);
	task->SetStatus(CTask::EtsQueued);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskSchedulerFifo::Dequeue
//
//	@doc:
//		Get next task to execute
//
//---------------------------------------------------------------------------
CTask *
CTaskSchedulerFifo::Dequeue()
{
	GPOS_ASSERT(!m_task_queue.IsEmpty());

	CTask *task = m_task_queue.RemoveHead();
	task->SetStatus(CTask::EtsDequeued);
	return task;
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskSchedulerFifo::Cancel
//
//	@doc:
//		Check if task is waiting to be scheduled and remove it
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTaskSchedulerFifo::Cancel(CTask *task)
{
	// iterate until found
	CTask *task_it = m_task_queue.First();
	while (NULL != task_it)
	{
		if (task_it == task)
		{
			m_task_queue.Remove(task_it);
			task_it->Cancel();

			return GPOS_OK;
		}
		task_it = m_task_queue.Next(task_it);
	}

	return GPOS_NOT_FOUND;
}