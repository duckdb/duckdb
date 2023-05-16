//---------------------------------------------------------------------------
//	@filename:
//		CAutoSuspendAbort.cpp
//
//	@doc:
//		Auto suspend abort object
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/task/CAutoSuspendAbort.h"
#include <stddef.h>
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CTask.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoSuspendAbort::CAutoSuspendAbort
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CAutoSuspendAbort::CAutoSuspendAbort()
{
	m_task = CTask::Self();

	if (NULL != m_task)
	{
		m_task->SuspendAbort();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoSuspendAbort::~CAutoSuspendAbort
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CAutoSuspendAbort::~CAutoSuspendAbort()
{
	if (NULL != m_task)
	{
		m_task->ResumeAbort();
	}
}