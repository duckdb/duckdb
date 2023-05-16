//---------------------------------------------------------------------------
//	@filename:
//		CAutoTimer.cpp
//
//	@doc:
//		Implementation of wrapper around wall clock timer
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CAutoTimer.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CAutoSuspendAbort.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoTimer::CAutoTimer
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CAutoTimer::CAutoTimer(const CHAR *sz, BOOL fPrint)
	: m_timer_text_label(sz), m_print_text_label(fPrint)
{
	GPOS_ASSERT(NULL != sz);
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoTimer::~CAutoTimer
//
//	@doc:
//		Destructor prints time difference and label
//
//---------------------------------------------------------------------------
CAutoTimer::~CAutoTimer() throw()
{
	if (m_print_text_label)
	{
		// suspend cancellation - destructors should not throw
		CAutoSuspendAbort asa;
		ULONG ulElapsedTimeMS = m_clock.ElapsedMS();
		GPOS_TRACE_FORMAT("timer:%s: %dms", m_timer_text_label, ulElapsedTimeMS);
	}
}