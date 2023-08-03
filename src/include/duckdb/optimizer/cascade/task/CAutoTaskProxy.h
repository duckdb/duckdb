//---------------------------------------------------------------------------
//	@filename:
//		CAutoTaskProxy.h
//
//	@doc:
//		Interface class for task management and execution.
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoTaskProxy_H
#define GPOS_CAutoTaskProxy_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/task/CTask.h"
#include "duckdb/optimizer/cascade/task/CWorkerPoolManager.h"
#include <list>

using namespace std;

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CAutoTaskProxy
//
//	@doc:
//		Auto task proxy (ATP) to handle task creation, execution and cleanup
//		ATP operations are not thread-safe; only one worker can use each ATP
//		object.
//
//---------------------------------------------------------------------------
class CAutoTaskProxy : CStackObject
{
private:
	// worker pool
	CWorkerPoolManager* m_pwpm;

	// task list
	list<duckdb::unique_ptr<CTask>> m_list;

	// propagate error of sub-task or not
	bool m_propagate_error;

	// find finished task
	GPOS_RESULT FindFinished(CTask** task);

public:
	// ctor
	CAutoTaskProxy(CWorkerPoolManager* m_pwpm, bool propagate_error = true);
	
	// no copy ctor
	CAutoTaskProxy(const CAutoTaskProxy &) = delete;
	
	// dtor
	~CAutoTaskProxy();

	// task count
	ULONG TaskCount()
	{
		return m_list.size();
	}

	// disable/enable error propagation
	void SetPropagateError(bool propagate_error)
	{
		m_propagate_error = propagate_error;
	}

	// create new task
	CTask* Create(void *(*pfunc)(void *), void *argv, bool *cancel = NULL);

	// schedule task for execution
	void Schedule(CTask* task);

	// execute task in thread owning ATP (synchronous execution)
	void Execute(CTask* task);

	// cancel task
	void Cancel(CTask* task);

	// unregister and release task
	void Destroy(duckdb::unique_ptr<CTask> task);

	// unregister and release all tasks
	void DestroyAll();
};	// class CAutoTaskProxy
}  // namespace gpos
#endif