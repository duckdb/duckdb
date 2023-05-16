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
#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolManager.h"

using namespace gpos;

//---------------------------------------------------------------------------
// static singleton - global instance of worker pool manager
//---------------------------------------------------------------------------
CWorkerPoolManager *CWorkerPoolManager::m_worker_pool_manager = NULL;


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::CWorkerPoolManager
//
//	@doc:
//		Private ctor
//
//---------------------------------------------------------------------------
CWorkerPoolManager::CWorkerPoolManager(CMemoryPool *mp)
	: m_mp(mp),
	  m_auto_task_proxy_counter(0),
	  m_active(false),
	  m_single_worker(NULL)
{
	// initialize hash table
	m_shtTS.Init(mp, GPOS_WORKERPOOL_HT_SIZE,
				 GPOS_OFFSET(CTask, m_worker_pool_manager_link),
				 GPOS_OFFSET(CTask, m_tid), &(CTaskId::m_invalid_tid),
				 CTaskId::HashValue, CTaskId::Equals);

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
GPOS_RESULT
CWorkerPoolManager::Init()
{
	GPOS_ASSERT(NULL == WorkerPoolManager());

	CMemoryPool *mp =
		CMemoryPoolManager::GetMemoryPoolMgr()->CreateMemoryPool();

	GPOS_TRY
	{
		// create worker pool
		CWorkerPoolManager::m_worker_pool_manager =
			GPOS_NEW(mp) CWorkerPoolManager(mp);
	}
	GPOS_CATCH_EX(ex)
	{
		// turn in memory pool in case of failure
		CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);

		CWorkerPoolManager::m_worker_pool_manager = NULL;

		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			return GPOS_OOM;
		}

		return GPOS_FAILED;
	}
	GPOS_CATCH_END;

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
void
CWorkerPoolManager::Shutdown()
{
	CWorkerPoolManager *worker_pool_manager =
		CWorkerPoolManager::m_worker_pool_manager;

	GPOS_ASSERT(NULL != worker_pool_manager &&
				"Worker pool has not been initialized");

	GPOS_ASSERT(0 == worker_pool_manager->m_auto_task_proxy_counter &&
				"AutoTaskProxy alive at worker pool shutdown");

	// stop scheduling tasks
	worker_pool_manager->m_active = false;

	CMemoryPool *mp = worker_pool_manager->m_mp;

	// destroy worker pool
	CWorkerPoolManager::m_worker_pool_manager = NULL;
	GPOS_DELETE(worker_pool_manager);

	// release allocated memory pool
	CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RegisterWorker()
//
//	@doc:
//		Insert worker into the WLS table
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::RegisterWorker(CWorker *worker)
{
	GPOS_ASSERT(NULL != worker);
	GPOS_ASSERT(NULL == m_single_worker);
	m_single_worker = worker;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RemoveWorker
//
//	@doc:
//		Remover worker, given its id, from WLS table
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::RemoveWorker()
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
void
CWorkerPoolManager::RegisterTask(CTask *task)
{
	GPOS_ASSERT(m_active && "Worker pool is not operating");

	// get access
	CTaskId &tid = task->m_tid;
	CSyncHashtableAccessByKey<CTask, CTaskId> shta(m_shtTS, tid);

	// must be first to register
	GPOS_ASSERT(NULL == shta.Find() && "Found registered task.");

	shta.Insert(task);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RemoveTask
//
//	@doc:
//		Remove worker, given by id, from the task table
//
//---------------------------------------------------------------------------
CTask *
CWorkerPoolManager::RemoveTask(CTaskId tid)
{
	CTask *task = NULL;

	// scope for hash table accessor
	{
		// get access
		CSyncHashtableAccessByKey<CTask, CTaskId> shta(m_shtTS, tid);

		task = shta.Find();
		if (NULL != task)
		{
			shta.Remove(task);
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
void
CWorkerPoolManager::Schedule(CTask *task)
{
	GPOS_ASSERT(m_active && "Worker pool is not operating");

	// add task to scheduler's queue
	m_task_scheduler.Enqueue(task);

	GPOS_CHECK_ABORT;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Cancel
//
//	@doc:
//		Mark task as canceled
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::Cancel(CTaskId tid)
{
	BOOL is_queued = false;

	CTask *task = NULL;

	// scope for hash table accessor
	{
		CSyncHashtableAccessByKey<CTask, CTaskId> shta(m_shtTS, tid);
		task = shta.Find();
		if (NULL != task)
		{
			task->Cancel();
			is_queued = (CTask::EtsQueued == task->m_status);
		}
	}

	// remove task from scheduler's queue
	if (is_queued)
	{
		GPOS_ASSERT(NULL != task);

		GPOS_RESULT eres = GPOS_OK;

		eres = m_task_scheduler.Cancel(task);

		// if task was dequeued, signal task completion
		if (GPOS_OK == eres)
		{
			task->SetStatus(CTask::EtsError);
		}
	}
}


// EOF
