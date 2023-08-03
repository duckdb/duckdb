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
void CTaskSchedulerFifo::Enqueue(CTask* task)
{
	m_task_queue.emplace_back(task);
	task->SetStatus(EtsQueued);
}

//---------------------------------------------------------------------------
//	@function:
//		CTaskSchedulerFifo::Dequeue
//
//	@doc:
//		Get next task to execute
//
//---------------------------------------------------------------------------
CTask* CTaskSchedulerFifo::Dequeue()
{
	CTask* task = *(m_task_queue.begin());
	m_task_queue.pop_front();
	task->SetStatus(EtsDequeued);
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
GPOS_RESULT CTaskSchedulerFifo::Cancel(CTask* task)
{
	// iterate until found
	auto itr = m_task_queue.begin();
	while (m_task_queue.end() != itr)
	{
		CTask* task_it = *itr;
		if (task_it == task)
		{
			m_task_queue.erase(itr);
			task_it->Cancel();
			return GPOS_OK;
		}
		++itr;
	}
	return GPOS_NOT_FOUND;
}