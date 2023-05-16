//---------------------------------------------------------------------------
//	@filename:
//		CWallClock.cpp
//
//	@doc:
//		Implementation of wall clock timer
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CWallClock.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/syslibwrapper.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CWallClock::ElapsedUS
//
//	@doc:
//		Wall-clock time in micro-seconds since object construction
//
//---------------------------------------------------------------------------
ULONG CWallClock::ElapsedUS() const
{
	timeval time;
	syslib::GetTimeOfDay(&time, NULL /*timezone*/);
	ULONG diff = (ULONG)(((time.tv_sec - m_time.tv_sec) * GPOS_USEC_IN_SEC) + (time.tv_usec - m_time.tv_usec));
	return diff;
}

//---------------------------------------------------------------------------
//	@function:
//		CWallClock::Restart
//
//	@doc:
//		Restart timer
//
//---------------------------------------------------------------------------
void CWallClock::Restart()
{
	syslib::GetTimeOfDay(&m_time, NULL /*timezone*/);
}