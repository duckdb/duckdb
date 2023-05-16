//---------------------------------------------------------------------------
//	@filename:
//		CTask.h
//
//	@doc:
//		Interface class for task abstraction
//---------------------------------------------------------------------------
#ifndef GPOS_CTask_H
#define GPOS_CTask_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/error/CErrorContext.h"
#include "duckdb/optimizer/cascade/error/CException.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolManager.h"
#include "duckdb/optimizer/cascade/task/CTaskContext.h"
#include "duckdb/optimizer/cascade/task/CTaskId.h"
#include "duckdb/optimizer/cascade/task/CTaskLocalStorage.h"
#include "duckdb/optimizer/cascade/task/ITask.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CTask
//
//	@doc:
//		Interface to abstract task (work unit);
//		provides asynchronous task execution and error handling;
//
//---------------------------------------------------------------------------
class CTask : public ITask
{
	friend class CAutoTaskProxy;
	friend class CAutoTaskProxyTest;
	friend class CTaskSchedulerFifo;
	friend class CWorker;
	friend class CWorkerPoolManager;
	friend class CUnittest;
	friend class CAutoSuspendAbort;

private:
	// task memory pool -- exclusively used by this task
	CMemoryPool *m_mp;

	// task context
	CTaskContext *m_task_ctxt;

	// error context
	IErrorContext *m_err_ctxt;

	// error handler stack
	CErrorHandler *m_err_handle;

	// function to execute
	void *(*m_func)(void *);

	// function argument
	void *m_arg;

	// function result
	void *m_res;

	// TLS
	CTaskLocalStorage m_tls;

	// task status
	ETaskStatus m_status;

	// cancellation flag
	BOOL *m_cancel;

	// local cancellation flag; used when no flag is externally passed
	BOOL m_cancel_local;

	// counter of requests to suspend cancellation
	ULONG m_abort_suspend_count;

	// flag denoting task completion report
	BOOL m_reported;

	// task identifier
	CTaskId m_tid;

	// ctor
	CTask(CMemoryPool *mp, CTaskContext *task_ctxt, IErrorContext *err_ctxt,
		  BOOL *cancel);

	// no copy ctor
	CTask(const CTask &);

	// binding a task structure to a function and its arguments
	void Bind(void *(*func)(void *), void *arg);

	// execution, called by the owning worker
	void Execute();

	// check if task has been scheduled
	BOOL IsScheduled() const;

	// check if task finished executing
	BOOL IsFinished() const;

	// check if task is currently executing
	BOOL
	IsRunning() const
	{
		return EtsRunning == m_status;
	}

	// reported flag accessor
	BOOL
	IsReported() const
	{
		return m_reported;
	}

	// set reported flag
	void
	SetReported()
	{
		GPOS_ASSERT(!m_reported && "Task already reported as completed");

		m_reported = true;
	}

public:
	// dtor
	virtual ~CTask();

	// accessor for memory pool, e.g. used for allocating task parameters in
	CMemoryPool *
	Pmp() const
	{
		return m_mp;
	}

	// TLS accessor
	CTaskLocalStorage &
	GetTls()
	{
		return m_tls;
	}

	// task id accessor
	CTaskId &
	GetTid()
	{
		return m_tid;
	}

	// task context accessor
	CTaskContext *
	GetTaskCtxt() const
	{
		return m_task_ctxt;
	}

	// basic output streams
	ILogger *
	GetOutputLogger() const
	{
		return this->m_task_ctxt->GetOutputLogger();
	}

	ILogger *
	GetErrorLogger() const
	{
		return this->m_task_ctxt->GetErrorLogger();
	}

	BOOL
	SetTrace(ULONG trace, BOOL val)
	{
		return this->m_task_ctxt->SetTrace(trace, val);
	}

	BOOL
	IsTraceSet(ULONG trace)
	{
		return this->m_task_ctxt->IsTraceSet(trace);
	}


	// locale
	ELocale
	Locale() const
	{
		return m_task_ctxt->Locale();
	}

	// check if task is canceled
	BOOL
	IsCanceled() const
	{
		return *m_cancel;
	}

	// reset cancel flag
	void
	ResetCancel()
	{
		*m_cancel = false;
	}

	// set cancel flag
	void
	Cancel()
	{
		*m_cancel = true;
	}

	// check if a request to suspend abort was received
	BOOL
	IsAbortSuspended() const
	{
		return (0 < m_abort_suspend_count);
	}

	// increment counter for requests to suspend abort
	void
	SuspendAbort()
	{
		m_abort_suspend_count++;
	}

	// decrement counter for requests to suspend abort
	void ResumeAbort();

	// task status accessor
	ETaskStatus
	GetStatus() const
	{
		return m_status;
	}

	// set task status
	void SetStatus(ETaskStatus status);

	// task result accessor
	void *
	GetRes() const
	{
		return m_res;
	}

	// error context
	IErrorContext *
	GetErrCtxt() const
	{
		return m_err_ctxt;
	}

	// error context
	CErrorContext *
	ConvertErrCtxt()
	{
		return dynamic_cast<CErrorContext *>(m_err_ctxt);
	}

	// pending exceptions
	BOOL
	HasPendingExceptions() const
	{
		return m_err_ctxt->IsPending();
	}

#ifdef GPOS_DEBUG
	// check if task has expected status
	BOOL CheckStatus(BOOL completed);
#endif	// GPOS_DEBUG

	// slink for auto task proxy
	SLink m_proxy_link;

	// slink for task scheduler
	SLink m_task_scheduler_link;

	// slink for worker pool manager
	SLink m_worker_pool_manager_link;

	static CTask *
	Self()
	{
		return dynamic_cast<CTask *>(ITask::Self());
	}

};	// class CTask

}  // namespace gpos

#endif