//---------------------------------------------------------------------------
//	@filename:
//		CWorker.cpp
//
//	@doc:
//		Worker abstraction, e.g. thread
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/common/syslibwrapper.h"
#include "duckdb/optimizer/cascade/string/CWStringStatic.h"
#include "duckdb/optimizer/cascade/task/CWorkerPoolManager.h"
#include <assert.h>

using namespace gpos;

// host system callback function to report abort requests
bool (*CWorker::abort_requested_by_system)(void);

//---------------------------------------------------------------------------
//	@function:
//		CWorker::CWorker
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CWorker::CWorker(ULONG stack_size, ULONG_PTR stack_start)
	: m_task(NULL), m_stack_size(stack_size), m_stack_start(stack_start)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::~CWorker
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CWorker::~CWorker()
{
	// unregister worker
	// CWorkerPoolManager::m_worker_pool_manager->RemoveWorker();
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::Execute
//
//	@doc:
//		Execute single task
//
//---------------------------------------------------------------------------
void CWorker::Execute(CTask* task)
{
	m_task = task;
	m_task->Execute();
	m_task = nullptr;
}

CWorker* CWorker::Self()
{
	CWorker *worker = nullptr;
	if (nullptr != CWorkerPoolManager::m_worker_pool_manager)
	{
		worker = CWorkerPoolManager::m_worker_pool_manager->m_single_worker.get();
	}
	return worker;
}