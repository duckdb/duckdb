//---------------------------------------------------------------------------
//	@filename:
//		ITaskScheduler.h
//
//	@doc:
//		Interface class for task scheduling
//---------------------------------------------------------------------------
#ifndef GPOS_ITaskScheduler_H
#define GPOS_ITaskScheduler_H

#include "duckdb/optimizer/cascade/types.h"

namespace gpos
{
// prototypes
class CTask;
class CTaskId;

//---------------------------------------------------------------------------
//	@class:
//		ITaskScheduler
//
//	@doc:
//		Interface for abstracting task scheduling primitives.
//
//---------------------------------------------------------------------------

class ITaskScheduler
{
public:
	// dummy ctor
	ITaskScheduler()
	{
	}
	
	// private copy ctor
	ITaskScheduler(const ITaskScheduler &) = delete;

	// dummy dtor
	virtual ~ITaskScheduler()
	{
	}

	// add task to waiting queue
	virtual void Enqueue(CTask* task) = 0;

	// get next task to execute
	virtual CTask* Dequeue() = 0;

	// check if task is waiting to be scheduled and remove it
	virtual GPOS_RESULT Cancel(CTask* task) = 0;

	// get number of waiting tasks
	virtual ULONG GetQueueSize() = 0;

	// check if task queue is empty
	virtual bool IsEmpty() const = 0;
};	// class ITaskScheduler
}  // namespace gpos
#endif