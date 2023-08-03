//---------------------------------------------------------------------------
//	@filename:
//		CTask.cpp
//
//	@doc:
//		Task implementation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/task/CTask.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"

using namespace gpos;

// init CTaskId's atomic counter
ULONG_PTR CTaskId::m_counter(0);

const CTaskId CTaskId::m_invalid_tid;

//---------------------------------------------------------------------------
//	@function:
//		CTask::CTask
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CTask::CTask()
	: m_func(NULL), m_arg(NULL), m_res(NULL), m_status(EtsInit), m_cancel(NULL), m_cancel_local(false), m_reported(false)
{
}

CTask::CTask(bool* cancel)
	: m_func(NULL), m_arg(NULL), m_res(NULL), m_status(EtsInit), m_cancel(cancel), m_cancel_local(false), m_reported(false)
{
	if (NULL == cancel)
	{
		m_cancel = &m_cancel_local;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTask::~CTask
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CTask::~CTask()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CTask::Bind
//
//	@doc:
//		Bind task to function and arguments
//
//---------------------------------------------------------------------------
void CTask::Bind(void *(*func)(void *), void *arg)
{
	m_func = func;
	m_arg = arg;
}

//---------------------------------------------------------------------------
//	@function:
//		CTask::Execute
//
//	@doc:
//		Execution of task function; wrapped in asserts to prevent leaks
//
//---------------------------------------------------------------------------
void CTask::Execute()
{
	// final task status
	ETaskStatus ets = m_status;
	// check for cancel
	if (*m_cancel)
	{
		ets = EtsError;
	}
	else
	{
		// mark task as running
		SetStatus(EtsRunning);
		// call executable function
		m_res = m_func(m_arg);
		// task completed
		ets = EtsCompleted;
	}
	// signal end of task execution
	SetStatus(ets);
}

//---------------------------------------------------------------------------
//	@function:
//		CTask::SetStatus
//
//	@doc:
//		Set task status;
//		Locking is required if updating more than one variable;
//
//---------------------------------------------------------------------------
void CTask::SetStatus(ETaskStatus ets)
{
	// status changes are monotonic
	m_status = ets;
}

//---------------------------------------------------------------------------
//	@function:
//  	CTask::IsScheduled
//
//	@doc:
//		Check if task has been scheduled
//
//---------------------------------------------------------------------------
bool CTask::IsScheduled() const
{
	switch (m_status)
	{
		case EtsInit:
			return false;
			break;
		case EtsQueued:
		case EtsDequeued:
		case EtsRunning:
		case EtsCompleted:
		case EtsError:
			return true;
			break;
		default:
			return false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTask::IsFinished
//
//	@doc:
//		Check if task finished executing
//
//---------------------------------------------------------------------------
bool CTask::IsFinished() const
{
	switch (m_status)
	{
		case EtsInit:
		case EtsQueued:
		case EtsDequeued:
		case EtsRunning:
			return false;
			break;
		case EtsCompleted:
		case EtsError:
			return true;
			break;
		default:
			return false;
	}
}

CTask* CTask::Self()
{
	CWorker* worker = CWorker::Self();
	if (nullptr != worker)
	{
		CTask* task = worker->GetTask();
		return task;
	}
	return nullptr;
}