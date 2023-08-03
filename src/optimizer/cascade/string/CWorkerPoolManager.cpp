//---------------------------------------------------------------------------
//	@filename:
//		CWorkerPoolManager.cpp
//
//	@doc:
//		Central worker pool manager;
//		* maintains worker local storage
//		* hosts task scheduler
//		* assigns tasks to workers
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/task/CWorkerPoolManager.h"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/helper.hpp"

using namespace duckdb;

namespace gpos
{
//---------------------------------------------------------------------------
// static singleton - global instance of worker pool manager
//---------------------------------------------------------------------------
duckdb::unique_ptr<CWorkerPoolManager> CWorkerPoolManager::m_worker_pool_manager = nullptr;

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::CWorkerPoolManager
//
//	@doc:
//		Private ctor
//
//---------------------------------------------------------------------------
CWorkerPoolManager::CWorkerPoolManager()
	: m_auto_task_proxy_counter(0), m_active(false), m_single_worker(nullptr)
{
	// set active
	m_active = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Init()
//
//	@doc:
//		Initializer for global worker pool manager
//
//---------------------------------------------------------------------------
GPOS_RESULT CWorkerPoolManager::Init()
{
	// create worker pool
	CWorkerPoolManager::m_worker_pool_manager = make_uniq<CWorkerPoolManager>();
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Shutdown
//
//	@doc:
//		Shutdown stops workers and cleans up worker pool memory
//
//---------------------------------------------------------------------------
void CWorkerPoolManager::Shutdown()
{
	// stop scheduling tasks
	CWorkerPoolManager::m_worker_pool_manager->m_active = false;
	// destroy worker pool
	CWorkerPoolManager::m_worker_pool_manager.release();
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RegisterWorker()
//
//	@doc:
//		Insert worker into the WLS table
//
//---------------------------------------------------------------------------
void CWorkerPoolManager::RegisterWorker(duckdb::unique_ptr<CWorker> worker)
{
	m_single_worker = std::move(worker);
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RemoveWorker
//
//	@doc:
//		Remover worker, given its id, from WLS table
//
//---------------------------------------------------------------------------
void CWorkerPoolManager::RemoveWorker()
{
	m_single_worker = NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RegisterTask()
//
//	@doc:
//		Insert a task into the task table
//
//---------------------------------------------------------------------------
void CWorkerPoolManager::RegisterTask(CTask* task)
{
	// get access
	CTaskId &tid = task->m_tid;
	m_shtTS.insert(make_pair(tid, task));
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RemoveTask
//
//	@doc:
//		Remove worker, given by id, from the task table
//
//---------------------------------------------------------------------------
CTask* CWorkerPoolManager::RemoveTask(CTaskId tid)
{
	CTask* task = nullptr;
	// scope for hash table accessor
	{
		// get access
		auto itr = m_shtTS.find(tid);
		if (m_shtTS.end() != itr)
		{
			task = itr->second;
			m_shtTS.erase(itr);
		}
	}
	return task;
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Schedule
//
//	@doc:
//		Add task to scheduler;
//
//---------------------------------------------------------------------------
void CWorkerPoolManager::Schedule(CTask* task)
{
	// add task to scheduler's queue
	m_task_scheduler.Enqueue(task);
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Cancel
//
//	@doc:
//		Mark task as canceled
//
//---------------------------------------------------------------------------
void CWorkerPoolManager::Cancel(CTaskId tid)
{
	bool is_queued = false;
	CTask* task = nullptr;
	// scope for hash table accessor
	{
		// get access
		auto itr = m_shtTS.find(tid);
		if (m_shtTS.end() != itr)
		{
			task = itr->second;
			task->Cancel();
			is_queued = (EtsQueued == task->m_status);
		}
	}
	// remove task from scheduler's queue
	if (is_queued)
	{
		GPOS_RESULT eres = GPOS_OK;
		eres = m_task_scheduler.Cancel(task);
		// if task was dequeued, signal task completion
		if (GPOS_OK == eres)
		{
			task->SetStatus(EtsError);
		}
	}
}
}