//---------------------------------------------------------------------------
//	@filename:
//		CTimerUser.h
//
//	@doc:
//		A timer which records elapsed user time;
//---------------------------------------------------------------------------
#ifndef GPOS_CTimerUser_H
#define GPOS_CTimerUser_H

#include "duckdb/optimizer/cascade/common/ITimer.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CTimerUser
//
//	@doc:
//		Records user time;
//
//---------------------------------------------------------------------------
class CTimerUser : public ITimer
{
private:
	// actual timer
	RUSAGE m_rusage;

public:
	// ctor
	CTimerUser()
	{
	}

	// retrieve elapsed user time in micro-seconds
	virtual ULONG ElapsedUS() const;

	// restart timer
	virtual void Restart();

};	// class CTimerUser
}  // namespace gpos

#endif
