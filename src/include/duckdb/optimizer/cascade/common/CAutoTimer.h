//---------------------------------------------------------------------------
//	@filename:
//		CAutoTimer.h
//
//	@doc:
//		A timer which records wall-time between construction and destruction;
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoTimer_H
#define GPOS_CAutoTimer_H

#include "duckdb/optimizer/cascade/common/CStackObject.h"
#include "duckdb/optimizer/cascade/common/CWallClock.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CAutoTimer
//
//	@doc:
//		Wrapper around timer object; prints elapsed time when going out of
//		scope as indicated (ctor argument);
//
//---------------------------------------------------------------------------
class CAutoTimer : public CStackObject
{
private:
	// actual timer
	CWallClock m_clock;

	// label for timer output
	const CHAR* m_timer_text_label;

	// trigger printing at destruction time
	bool m_print_text_label;

	// private copy ctor
	CAutoTimer(const CAutoTimer &);

public:
	// ctor
	CAutoTimer(const CHAR* sz, bool fPrint);

	// dtor
	~CAutoTimer() throw();
};	// class CAutoTimer
}  // namespace gpos
#endif