//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CStateMachine.h
//
//	@doc:
//		Basic FSM implementation;
//		This superclass provides the API and handles only the bookkeeping;
//
//		Each subclass has to provide a transition function which encodes
//		the actual state graph and determines validity of transitions;
//
//		Provides debug functionality to draw state diagram and validation
//		function to detect unreachable states.
//---------------------------------------------------------------------------
#ifndef GPOPT_CStateMachine_H
#define GPOPT_CStateMachine_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/types.h"

// maximum length of names used for states and events
#define GPOPT_FSM_NAME_LENGTH 128

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CStateMachine
//
//	@doc:
//		Abstract FSM which accepts events to manage state transitions;
//		Corresponds to a Moore Machine;
//
//---------------------------------------------------------------------------
template <class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent, TEnumEvent tenumeventSentinel>
class CStateMachine
{
public:
	// current state
	TEnumState m_tenumstate;

	// flag indicating if the state machine is initialized
	bool m_fInit;

	// array of transitions
	TEnumEvent m_rgrgtenumeventTransitions[tenumstateSentinel][tenumstateSentinel];

public:
	// ctor
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>()
		: m_tenumstate(TesInitial()), m_fInit(false)
	{
	}

	// hidden copy ctor
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel>(const CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent, tenumeventSentinel> &) = delete;
	
	// initialize state machine
	void Init(const TEnumEvent rgrgtenumeventTransitions[tenumstateSentinel][tenumstateSentinel])
	{
		for (ULONG ulOuter = 0; ulOuter < tenumstateSentinel; ulOuter++)
		{
			for (ULONG ulInner = 0; ulInner < tenumstateSentinel; ulInner++)
			{
				m_rgrgtenumeventTransitions[ulOuter][ulInner] = rgrgtenumeventTransitions[ulOuter][ulInner];
			}
		}
		m_tenumstate = TesInitial();
		m_fInit = true;
	}

	// dtor
	~CStateMachine()
	{
	}

	// actual implementation of transition
	bool FAttemptTransition(TEnumState tenumstateOld, TEnumEvent tenumevent, TEnumState &tenumstateNew) const
	{
		for (ULONG ulOuter = 0; ulOuter < tenumstateSentinel; ulOuter++)
		{
			if (m_rgrgtenumeventTransitions[tenumstateOld][ulOuter] == tenumevent)
			{
				tenumstateNew = (TEnumState) ulOuter;
				return true;
			}
		}
		return false;
	}

	// attempt transition
	bool FTransition(TEnumEvent tenumevent, TEnumState &tenumstate)
	{
		TEnumState tenumstateNew;
		bool fSucceeded = FAttemptTransition(m_tenumstate, tenumevent, tenumstateNew);
		if (fSucceeded)
		{
			m_tenumstate = tenumstateNew;
		}
		tenumstate = m_tenumstate;
		return fSucceeded;
	}

	// shorthand if current state and return value are not needed
	void Transition(TEnumEvent tenumevent)
	{
		TEnumState tenumstateDummy;
		(void) FTransition(tenumevent, tenumstateDummy);
	}

	// inspect current state; to be used only in assertions
	TEnumState Estate() const
	{
		return m_tenumstate;
	}

	// get initial state
	TEnumState TesInitial() const
	{
		return (TEnumState) 0;
	}

	// get final state
	TEnumState TesFinal() const
	{
		return (TEnumState)(tenumstateSentinel - 1);
	}

	// reset state
	void Reset()
	{
		m_tenumstate = TesInitial();
		m_fInit = false;
	}
};	// class CStateMachine
}  // namespace gpopt
#endif