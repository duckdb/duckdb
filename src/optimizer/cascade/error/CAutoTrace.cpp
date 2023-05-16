//---------------------------------------------------------------------------
//	@filename:
//		CAutoTrace.cpp
//
//	@doc:
//		Implementation of auto object for creating trace messages
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoTrace::CAutoTrace
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CAutoTrace::CAutoTrace(CMemoryPool *mp) : m_wstr(mp), m_os(&m_wstr)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoTrace::~CAutoTrace
//
//	@doc:
//		Destructor prints trace message
//
//---------------------------------------------------------------------------
CAutoTrace::~CAutoTrace()
{
	if (0 < m_wstr.Length() && !ITask::Self()->GetErrCtxt()->IsPending())
	{
		GPOS_TRACE(m_wstr.GetBuffer());
	}
}