//-----------------------------------------------------------------------------
//	@filename:
//		CWorkerPoolManager.h
//
//	@doc:
//		Central worker pool manager;
//		* maintains worker local storage
//		* hosts task scheduler
//		* assigns tasks to workers
//-----------------------------------------------------------------------------
#ifndef GPOS_CWorkerPoolManager_H
#define GPOS_CWorkerPoolManager_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CTask.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/task/CTaskId.h"
#include "duckdb/optimizer/cascade/task/CTaskSchedulerFifo.h"
#include "duckdb/common/unique_ptr.hpp"
#include <unordered_map>

#define GPOS_WORKERPOOL_HT_SIZE (1024)		 // number of buckets in hash tables
#define GPOS_WORKER_STACK_SIZE (500 * 1024)	 // max worker stack size

using namespace duckdb;

namespace gpos
{
//------------------------------------------------------------------------
//	@class:
//		CWorkerPoolManager
//
//	@doc:
//		Singleton object to handle worker pool;
//		maintains WLS (worker local storage);
//		assigns tasks to workers;
//
//------------------------------------------------------------------------
class CWorkerPoolManager
{
public:
	// response to worker scheduling request
	enum EScheduleResponse
	{ EsrExecTask, EsrWorkerExit };

public:
	// task scheduler
	CTaskSchedulerFifo m_task_scheduler;

	// auto task proxy counter
	ULONG_PTR m_auto_task_proxy_counter;

	// active flag
	bool m_active;

	// we only support a single worker now
	duckdb::unique_ptr<CWorker> m_single_worker;

	// task storage
	unordered_map<CTaskId, CTask*, CTaskId::CTaskIdHash> m_shtTS;

public:
	//-------------------------------------------------------------------
	// Interface for CAutoTaskProxy
	//-------------------------------------------------------------------
	// add task to scheduler
	void Schedule(CTask* task);

	// increment AutoTaskProxy reference counter
	void AddRef()
	{
		m_auto_task_proxy_counter++;
	}

	// decrement AutoTaskProxy reference counter
	void RemoveRef()
	{
		m_auto_task_proxy_counter--;
	}

	// insert task in table
	void RegisterTask(CTask* task);

	// remove task from table
	CTask* RemoveTask(CTaskId tid);

	//-------------------------------------------------------------------
	// Interface for CWorker
	//-------------------------------------------------------------------
	// insert worker in table
	void RegisterWorker(duckdb::unique_ptr<CWorker> worker);

	// remove worker from table
	void RemoveWorker();

	// static singleton - global instance of worker pool manager
	static duckdb::unique_ptr<CWorkerPoolManager> m_worker_pool_manager;

public:
	//-------------------------------------------------------------------
	// Methods for internal use
	//-------------------------------------------------------------------
	// private ctor
	CWorkerPoolManager();

	// no copy ctor
	CWorkerPoolManager(const CWorkerPoolManager &) = delete;

	// dtor
	~CWorkerPoolManager()
	{
	}

	// initialize worker pool manager
	static GPOS_RESULT Init();

	// de-init global instance
	static void Shutdown();

	// cancel task by task id
	void Cancel(CTaskId tid);
};	// class CWorkerPoolManager
}  // namespace gpos
#endif