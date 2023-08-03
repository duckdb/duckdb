//---------------------------------------------------------------------------
//	@filename:
//		CAutoTimer.cpp
//
//	@doc:
//		Implementation of wrapper around wall clock timer
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CAutoTimer.h"
#include "duckdb/optimizer/cascade/base.h"

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
}