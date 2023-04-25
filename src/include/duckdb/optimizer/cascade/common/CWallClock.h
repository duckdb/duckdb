//---------------------------------------------------------------------------
//	@filename:
//		CWallClock.h
//
//	@doc:
//		A timer which records wall clock time;
//---------------------------------------------------------------------------
#ifndef GPOS_CWallClock_H
#define GPOS_CWallClock_H

#include "duckdb/optimizer/cascade/common/ITimer.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CWallClock
//
//	@doc:
//		Records wall clock time;
//
//---------------------------------------------------------------------------
class CWallClock : public ITimer
{
private:
	// actual timer
	TIMEVAL m_time;

public:
	// ctor
	CWallClock()
	{
		Restart();
	}

	// retrieve elapsed wall-clock time in micro-seconds
	virtual ULONG ElapsedUS() const;

	// restart timer
	virtual void Restart();
};

}  // namespace gpos

#endif
