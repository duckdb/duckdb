//---------------------------------------------------------------------------
//	@filename:
//		CAutoTaskProxy.cpp
//
//	@doc:
//		Implementation of interface class for task management and execution.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/common/CWallClock.h"
#include "duckdb/common/helper.hpp"
#include <algorithm>

using namespace std;

namespace gpos
{
//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::CAutoTaskProxy
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CAutoTaskProxy::CAutoTaskProxy(CWorkerPoolManager* pwpm, bool propagate_error)
	: m_pwpm(pwpm), m_propagate_error(propagate_error)
{
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
	while (!m_list.empty())
	{
		auto itr = m_list.begin();
		Destroy(std::move(*itr));
		m_list.pop_front();
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
void CAutoTaskProxy::Destroy(duckdb::unique_ptr<CTask> task)
{
	// cancel scheduled task
	if (task->IsScheduled() && !task->IsReported())
	{
		Cancel(task.get());
		task->SetReported();
	}
	// unregister task from worker pool
	m_pwpm->RemoveTask(task->GetTid());
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
CTask* CAutoTaskProxy::Create(void *(*pfunc)(void *), void *arg, bool *cancel)
{
	duckdb::unique_ptr<CTask> new_task = make_uniq<CTask>(cancel);
	CTask* ptr = new_task.get();
	new_task->Bind(pfunc, arg);
	// add to task list
	m_list.emplace_back(std::move(new_task));
	// register task to worker pool
	m_pwpm->RegisterTask(ptr);
	return ptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Schedule
//
//	@doc:
//		Schedule task for execution
//
//---------------------------------------------------------------------------
void CAutoTaskProxy::Schedule(CTask* task)
{
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
GPOS_RESULT CAutoTaskProxy::FindFinished(CTask** task)
{
	*task = nullptr;
	// iterate task list
	for (auto cur_itr = m_list.begin(); m_list.end() != cur_itr; cur_itr++)
	{
		auto cur_task = (*cur_itr).get();
		// check if task has been reported as finished
		if (!cur_task->IsReported())
		{
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
void CAutoTaskProxy::Execute(CTask* task)
{
	// mark task as ready to execute
	task->SetStatus(EtsDequeued);
	// get worker of current thread
	CWorker::Self()->m_task = task;
	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Cancel
//
//	@doc:
//		Cancel task
//
//---------------------------------------------------------------------------
void CAutoTaskProxy::Cancel(CTask* task)
{
	if (!task->IsFinished())
	{
		m_pwpm->Cancel(task->GetTid());
	}
}
}