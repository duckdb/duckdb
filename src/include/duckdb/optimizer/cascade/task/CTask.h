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
#include "duckdb/optimizer/cascade/task/CTaskId.h"
#include "duckdb/optimizer/cascade/task/CTaskLocalStorage.h"

namespace gpos
{
// task status
enum ETaskStatus
{ EtsInit, EtsQueued, EtsDequeued, EtsRunning, EtsCompleted, EtsError };

//---------------------------------------------------------------------------
//	@class:
//		CTask
//
//	@doc:
//		Interface to abstract task (work unit);
//		provides asynchronous task execution and error handling;
//
//---------------------------------------------------------------------------
class CTask
{
public:
	// TLS
	CTaskLocalStorage m_tls;

	// task status
	ETaskStatus m_status;

	// cancellation flag
	bool* m_cancel;

	// local cancellation flag; used when no flag is externally passed
	bool m_cancel_local;

	// flag denoting task completion report
	bool m_reported;

	// task identifier
	CTaskId m_tid;

	// function to execute
	void* (*m_func)(void *);

	// function argument
	void* m_arg;

	// function result
	void* m_res;

public:
	CTask();

	// ctor
	CTask(bool* cancel);

	// no copy ctor
	CTask(const CTask &) = delete;

	// dtor
	virtual ~CTask();

public:
	// binding a task structure to a function and its arguments
	void Bind(void* (*func)(void *), void* arg);

	// execution, called by the owning worker
	void Execute();

	// check if task has been scheduled
	bool IsScheduled() const;

	// check if task finished executing
	bool IsFinished() const;

	// check if task is currently executing
	bool IsRunning() const
	{
		return EtsRunning == m_status;
	}

	// reported flag accessor
	bool IsReported() const
	{
		return m_reported;
	}

	// set reported flag
	void SetReported()
	{
		m_reported = true;
	}

	// TLS accessor
	CTaskLocalStorage & GetTls()
	{
		return m_tls;
	}

	// task id accessor
	CTaskId & GetTid()
	{
		return m_tid;
	}

	// check if task is canceled
	bool IsCanceled() const
	{
		return *m_cancel;
	}

	// reset cancel flag
	void ResetCancel()
	{
		*m_cancel = false;
	}

	// set cancel flag
	void Cancel()
	{
		*m_cancel = true;
	}

	// task status accessor
	ETaskStatus GetStatus() const
	{
		return m_status;
	}

	// set task status
	void SetStatus(ETaskStatus status);

	// task result accessor
	void* GetRes() const
	{
		return m_res;
	}

	// slink for auto task proxy
	SLink m_proxy_link;

	// slink for task scheduler
	SLink m_task_scheduler_link;

	// slink for worker pool manager
	SLink m_worker_pool_manager_link;

	static CTask* Self();
};	// class CTask
}  // namespace gpos
#endif