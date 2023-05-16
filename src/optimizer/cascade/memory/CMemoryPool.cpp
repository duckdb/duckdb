//---------------------------------------------------------------------------
//	@filename:
//		CMemoryPool.cpp
//
//	@doc:
//		Implementation of abstract interface;
//		implements helper functions for extraction of allocation
//		header from memory block;
//---------------------------------------------------------------------------

#ifdef GPOS_DEBUG
#include "duckdb/optimizer/cascade/error/CFSimulator.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolTracker.h"
#include "duckdb/optimizer/cascade/memory/CMemoryVisitorPrint.h"
#endif	// GPOS_DEBUG

#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolManager.h"
#include "duckdb/optimizer/cascade/task/ITask.h"

using namespace gpos;

// invalid exception
const ULONG_PTR CMemoryPool::m_invalid = ULONG_PTR_MAX;

// get user requested size of allocation
ULONG
CMemoryPool::UserSizeOfAlloc(const void *ptr)
{
	GPOS_ASSERT(NULL != ptr);

	return CMemoryPoolManager::GetMemoryPoolMgr()->UserSizeOfAlloc(ptr);
}


void
CMemoryPool::DeleteImpl(void *ptr, EAllocationType eat)
{
	CMemoryPoolManager::GetMemoryPoolMgr()->DeleteImpl(ptr, eat);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::Print
//
//	@doc:
//		Walk all objects and print identification
//
//---------------------------------------------------------------------------
IOstream &
CMemoryPool::OsPrint(IOstream &os)
{
	os << "Memory pool: " << this;

	ITask *task = ITask::Self();
	if (NULL != task && task->IsTraceSet(EtracePrintMemoryLeakStackTrace))
	{
		os << ", stack trace: " << std::endl;
		m_stack_desc.AppendTrace(os, 8 /*ulDepth*/);
	}
	else
	{
		os << std::endl;
	}

	if (SupportsLiveObjectWalk())
	{
		CMemoryVisitorPrint visitor(os);
		WalkLiveObjects(&visitor);
	}

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::AssertEmpty
//
//	@doc:
//		Static helper function to print out if the given pool is empty;
//		if the pool is not empty and we are compiled with debugging on then
//		an assertion will be thrown;
//
//---------------------------------------------------------------------------
void
CMemoryPool::AssertEmpty(IOstream &os)
{
	if (SupportsLiveObjectWalk() && NULL != ITask::Self() &&
		!GPOS_FTRACE(EtraceDisablePrintMemoryLeak))
	{
		CMemoryVisitorPrint visitor(os);
		WalkLiveObjects(&visitor);

		if (0 != visitor.GetNumVisits())
		{
			os << "Unfreed memory in memory pool " << (void *) this << ": "
			   << visitor.GetNumVisits() << " objects leaked" << std::endl;

			GPOS_ASSERT(!"leak detected");
		}
	}
}

#endif