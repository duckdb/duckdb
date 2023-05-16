//---------------------------------------------------------------------------
//	@filename:
//		CTaskContext.cpp
//
//	@doc:
//		Task context implementation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/task/CTaskContext.h"
#include "duckdb/optimizer/cascade/common/CAutoRef.h"
#include "duckdb/optimizer/cascade/error/CLoggerStream.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::CTaskContext
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CTaskContext::CTaskContext(CMemoryPool *mp)
	: m_bitset(NULL),
	  m_log_out(&CLoggerStream::m_stdout_stream_logger),
	  m_log_err(&CLoggerStream::m_stderr_stream_logger),
	  m_locale(ElocEnUS_Utf8)
{
	m_bitset = GPOS_NEW(mp) CBitSet(mp, EtraceSentinel);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::CTaskContext
//
//	@doc:
//		used to inherit parent task's context
//
//---------------------------------------------------------------------------
CTaskContext::CTaskContext(CMemoryPool *mp, const CTaskContext &task_ctxt)
	: m_bitset(NULL),
	  m_log_out(task_ctxt.GetOutputLogger()),
	  m_log_err(task_ctxt.GetErrorLogger()),
	  m_locale(task_ctxt.Locale())
{
	// allocate bitset and union separately to guard against leaks under OOM
	CAutoRef<CBitSet> bitset;

	bitset = GPOS_NEW(mp) CBitSet(mp);
	bitset->Union(task_ctxt.m_bitset);

	m_bitset = bitset.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::~CTaskContext
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CTaskContext::~CTaskContext()
{
	CRefCount::SafeRelease(m_bitset);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::Trace
//
//	@doc:
//		Set trace flag; return original setting
//
//---------------------------------------------------------------------------
BOOL
CTaskContext::SetTrace(ULONG trace, BOOL val)
{
	if (val)
	{
		return m_bitset->ExchangeSet(trace);
	}
	else
	{
		return m_bitset->ExchangeClear(trace);
	}
}