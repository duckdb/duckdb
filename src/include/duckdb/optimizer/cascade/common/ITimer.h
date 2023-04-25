//---------------------------------------------------------------------------
//	@filename:
//		ITimer.h
//
//	@doc:
//		A timer which records time between construction and the ElapsedMS call;
//---------------------------------------------------------------------------
#ifndef GPOS_ITimer_H
#define GPOS_ITimer_H

#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		ITimer
//
//	@doc:
//		Timer interface;
//
//---------------------------------------------------------------------------
class ITimer
{
private:
	// private copy ctor
	ITimer(const ITimer &);

public:
	// ctor
	ITimer()
	{
	}

	// dtor
	virtual ~ITimer()
	{
	}

	// retrieve elapsed time in micro-seconds
	virtual ULONG ElapsedUS() const = 0;

	// retrieve elapsed time in milli-seconds
	ULONG
	ElapsedMS() const
	{
		return ElapsedUS() / GPOS_USEC_IN_MSEC;
	}

	// restart timer
	virtual void Restart() = 0;

};	// class ITimer

}  // namespace gpos

#endif
