//---------------------------------------------------------------------------
//	@filename:
//		CAutoTaskProxy.cpp
//
//	@doc:
//		Implementation of interface class for task management and execution.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"
#include "duckdb/optimizer/cascade/common/CAutoP.h"
#include "duckdb/optimizer/cascade/common/CWallClock.h"
#include "duckdb/optimizer/cascade/error/CErrorContext.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/task/CAutoSuspendAbort.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::CAutoTaskProxy
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CAutoTaskProxy::CAutoTaskProxy(CMemoryPool *mp, CWorkerPoolManager *pwpm, BOOL propagate_error)
	: m_mp(mp), m_pwpm(pwpm), m_propagate_error(propagate_error)
{
	m_list.Init(GPOS_OFFSET(CTask, m_proxy_link));
	// register new ATP to worker pool
	m_pwpm->AddRef();
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::~CAutoTaskProxy
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CAutoTaskProxy::~CAutoTaskProxy()
{
	// suspend cancellation - destructors should not throw
	CAutoSuspendAbort asa;

	// disable error propagation from sub-task
	SetPropagateError(false);

	// destroy all tasks
	DestroyAll();

	// remove ATP from worker pool
	m_pwpm->RemoveRef();
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::DestroyAll
//
//	@doc:
//		Unregister and release all tasks
//
//---------------------------------------------------------------------------
void CAutoTaskProxy::DestroyAll()
{
	// iterate task list
	while (!m_list.IsEmpty())
	{
		Destroy(m_list.First());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Destroy
//
//	@doc:
//		Unregister and release task
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Destroy(CTask *task)
{
	// cancel scheduled task
	if (task->IsScheduled() && !task->IsReported())
	{
		Cancel(task);
		task->SetReported();
		CheckError(task);
	}

	// unregister task from worker pool
	m_pwpm->RemoveTask(task->GetTid());

	// remove task from list
	m_list.Remove(task);

	// delete task object
	GPOS_DELETE(task);
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Create
//
//	@doc:
//		Create new task;
//		Bind task to function and argument and associate with task and error context;
//		If caller is a task, its task context is cloned and used by the new task;
//
//---------------------------------------------------------------------------
CTask *
CAutoTaskProxy::Create(void *(*pfunc)(void *), void *arg, BOOL *cancel)
{
	// create memory pool for task
	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	CMemoryPool *mp = amp.Pmp();

	// auto pointer to hold new task context
	CAutoP<CTaskContext> task_ctxt;

	// check if caller is a task
	ITask *task_parent = CWorker::Self()->GetTask();
	if (NULL == task_parent)
	{
		// create new task context
		task_ctxt = GPOS_NEW(mp) CTaskContext(mp);
	}
	else
	{
		// clone parent task's context
		task_ctxt = GPOS_NEW(mp) CTaskContext(mp, *task_parent->GetTaskCtxt());
	}

	// auto pointer to hold error context
	CAutoP<CErrorContext> err_ctxt;
	err_ctxt = GPOS_NEW(mp) CErrorContext();
	CTask *task = CTask::Self();
	if (NULL != task)
	{
		err_ctxt.Value()->Register(task->ConvertErrCtxt()->GetMiniDumper());
	}

	// auto pointer to hold new task
	// task is created inside ATP's memory pool
	CAutoP<CTask> new_task;
	new_task =
		GPOS_NEW(m_mp) CTask(mp, task_ctxt.Value(), err_ctxt.Value(), cancel);

	// reset auto pointers - task now handles task and error context
	(void) task_ctxt.Reset();
	(void) err_ctxt.Reset();

	// detach task's memory pool from auto memory pool
	amp.Detach();

	// bind function and argument
	task = new_task.Value();
	task->Bind(pfunc, arg);

	// add to task list
	m_list.Append(task);

	// reset auto pointer - ATP now handles task
	new_task.Reset();

	// register task to worker pool
	m_pwpm->RegisterTask(task);

	return task;
}



//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Schedule
//
//	@doc:
//		Schedule task for execution
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Schedule(CTask *task)
{
	GPOS_ASSERT(CTask::EtsInit == task->m_status && "Task already scheduled");

	m_pwpm->Schedule(task);
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::FindFinished
//
//	@doc:
//		Find finished task
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxy::FindFinished(CTask **task)
{
	*task = NULL;

#ifdef GPOS_DEBUG
	// check if there is any task scheduled
	BOOL scheduled = false;

	// check if all tasks have been reported as finished
	BOOL reported_all = true;
#endif	// GPOS_DEBUG

	// iterate task list
	for (CTask *cur_task = m_list.First(); NULL != cur_task;
		 cur_task = m_list.Next(cur_task))
	{
#ifdef GPOS_DEBUG
		// check if task has been scheduled
		if (cur_task->IsScheduled())
		{
			scheduled = true;
		}
#endif	// GPOS_DEBUG

		// check if task has been reported as finished
		if (!cur_task->IsReported())
		{
#ifdef GPOS_DEBUG
			reported_all = false;
#endif	// GPOS_DEBUG

			// check if task is finished
			if (cur_task->IsFinished())
			{
				// mark task as reported
				cur_task->SetReported();
				*task = cur_task;

				return GPOS_OK;
			}
		}
	}

	GPOS_ASSERT(scheduled && "No task scheduled yet");
	GPOS_ASSERT(!reported_all && "All tasks have been reported as finished");

	return GPOS_NOT_FOUND;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Execute
//
//	@doc:
//		Execute task in thread owning ATP (synchronous execution);
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Execute(CTask *task)
{
	GPOS_ASSERT(CTask::EtsInit == task->m_status && "Task already scheduled");

	// mark task as ready to execute
	task->SetStatus(CTask::EtsDequeued);

	GPOS_TRY
	{
		// get worker of current thread
		CWorker *worker = CWorker::Self();
		GPOS_ASSERT(NULL != worker);

		// execute task
		worker->Execute(task);
	}
	GPOS_CATCH_EX(ex)
	{
		// mark task as erroneous
		task->SetStatus(CTask::EtsError);

		if (m_propagate_error)
		{
			GPOS_RETHROW(ex);
		}
	}
	GPOS_CATCH_END;

	// Raise exception if task encounters an exception
	if (task->HasPendingExceptions())
	{
		if (m_propagate_error)
		{
			GPOS_RETHROW(task->GetErrCtxt()->GetException());
		}
		else
		{
			task->GetErrCtxt()->Reset();
		}
	}

	// mark task as reported
	task->SetReported();
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Cancel
//
//	@doc:
//		Cancel task
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Cancel(CTask *task)
{
	if (!task->IsFinished())
	{
		m_pwpm->Cancel(task->GetTid());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::CheckError
//
//	@doc:
//		Check error from sub-task
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::CheckError(CTask *sub_task)
{
	// sub-task has a pending error
	if (sub_task->HasPendingExceptions())
	{
		// must be in error status
		GPOS_ASSERT(ITask::EtsError == sub_task->GetStatus());

		if (m_propagate_error)
		{
			// propagate error from sub task to current task
			PropagateError(sub_task);
		}
		else
		{
			// ignore the pending error from sub task
			// and reset its error context
			sub_task->GetErrCtxt()->Reset();
		}
	}
#ifdef GPOS_DEBUG
	else if (ITask::EtsError == sub_task->GetStatus())
	{
		// sub-task was canceled without a pending error
		GPOS_ASSERT(!sub_task->HasPendingExceptions() &&
					sub_task->IsCanceled());
	}
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::PropagateError
//
//	@doc:
//		Propagate the error from sub task to current task
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::PropagateError(CTask *sub_task)
{
	GPOS_ASSERT(m_propagate_error);

	// sub-task must be in error status and have a pending exception
	GPOS_ASSERT(ITask::EtsError == sub_task->GetStatus() &&
				sub_task->HasPendingExceptions());

	CTask *current_task = CTask::Self();

	// current task must have no pending error
	GPOS_ASSERT(NULL != current_task && !current_task->HasPendingExceptions());

	IErrorContext *current_err_ctxt = current_task->GetErrCtxt();

	// copy necessary error info for propagation
	current_err_ctxt->CopyPropErrCtxt(sub_task->GetErrCtxt());

	// reset error of sub task
	sub_task->GetErrCtxt()->Reset();

	// propagate the error
	CException::Reraise(current_err_ctxt->GetException(), true /*propagate*/);
}