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
#include "duckdb/optimizer/cascade/common/CEnumSet.h"
#include "duckdb/optimizer/cascade/types.h"

#ifdef GPOS_DEBUG
#include "duckdb/optimizer/cascade/common/CEnumSetIter.h"

// history of past N states/events
#define GPOPT_FSM_HISTORY 25u

#endif	// GPOS_DEBUG

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
template <class TEnumState, TEnumState tenumstateSentinel, class TEnumEvent,
		  TEnumEvent tenumeventSentinel>
class CStateMachine
{
private:
#ifdef GPOS_DEBUG
	// shorthand for sets and iterators
	typedef CEnumSet<TEnumState, tenumstateSentinel> EsetStates;
	typedef CEnumSet<TEnumEvent, tenumeventSentinel> EsetEvents;

	typedef CEnumSetIter<TEnumState, tenumstateSentinel> EsetStatesIter;
	typedef CEnumSetIter<TEnumEvent, tenumeventSentinel> EsetEventsIter;
#endif	// GPOS_DEBUG

	// current state
	TEnumState m_tenumstate;

	// flag indicating if the state machine is initialized
	BOOL m_fInit;

	// array of transitions
	TEnumEvent m_rgrgtenumeventTransitions[tenumstateSentinel]
										  [tenumstateSentinel];

#ifdef GPOS_DEBUG

#define GRAPHVIZ_SHAPE(shape, x) \
	"node [shape = " << shape << " ]; \"" << x << "\" ; node [shape = circle];"

#define GRAPHVIZ_DOUBLE_CIRCLE(x) GRAPHVIZ_SHAPE("doublecircle", x)
#define GRAPHVIZ_BOX(x) GRAPHVIZ_SHAPE("box", x)

	// state names
	const WCHAR *m_rgwszStates[tenumstateSentinel];

	// event names
	const WCHAR *m_rgwszEvents[tenumeventSentinel];

	// current index into history
	ULONG m_ulHistory;

	// state history
	TEnumState m_tenumstateHistory[GPOPT_FSM_HISTORY];

	// event history
	TEnumEvent m_tenumeventHistory[GPOPT_FSM_HISTORY];

	// track event
	void
	RecordHistory(TEnumEvent tenumevent, TEnumState tenumstate)
	{
		ULONG ulHistory = m_ulHistory % GPOPT_FSM_HISTORY;

		m_tenumeventHistory[ulHistory] = tenumevent;
		m_tenumstateHistory[ulHistory] = tenumstate;

		++m_ulHistory;
	}

	// resolve names for states
	const WCHAR *
	WszState(TEnumState tenumstate) const
	{
		GPOS_ASSERT(m_fInit);
		GPOS_ASSERT(0 <= tenumstate && tenumstate < tenumstateSentinel);

		return m_rgwszStates[tenumstate];
	}

	// resolve names for events
	const WCHAR *
	WszEvent(TEnumEvent tenumevent) const
	{
		GPOS_ASSERT(m_fInit);
		GPOS_ASSERT(0 <= tenumevent && tenumevent < tenumeventSentinel);

		return m_rgwszEvents[tenumevent];
	}

	// retrieve all states -- enum might have 'holes'
	void
	States(EsetStates *peset) const
	{
		for (ULONG ul = 0; ul < tenumstateSentinel; ul++)
		{
			(void) peset->ExchangeSet((TEnumState) ul);
		}
	}

	// determine all possible transitions between 2 given states
	void
	Transitions(TEnumState tenumstateOld, TEnumState tenumstateNew,
				EsetEvents *peset) const
	{
		TEnumEvent tenumevent =
			m_rgrgtenumeventTransitions[tenumstateOld][tenumstateNew];
		if (tenumeventSentinel != tenumevent)
		{
			(void) peset->ExchangeSet(tenumevent);
		}
	}

	// shorthand for walker function type
	typedef void (*PfWalker)(const CStateMachine *psm, TEnumState tenumstateOld,
							 TEnumState tenumstateNew, TEnumEvent tenumevent,
							 void *pvContext);

	// generic walker function, called for every edge in the graph
	void
	Walk(CMemoryPool *mp, PfWalker Pfpv, void *pvContext) const
	{
		// retrieve all states
		EsetStates *pesetStates = GPOS_NEW(mp) EsetStates(mp);
		States(pesetStates);

		// loop through all sink states
		EsetStatesIter esetIterSink(*pesetStates);
		while (esetIterSink.Advance())
		{
			TEnumState tenumstateSink = esetIterSink.TBit();

			// loop through all source states
			EsetStatesIter esetIterSource(*pesetStates);
			while (esetIterSource.Advance())
			{
				TEnumState tenumstateSource = esetIterSource.TBit();

				// for all pairs of states (source, sink)
				// compute possible transitions
				EsetEvents *pesetEvents = GPOS_NEW(mp) EsetEvents(mp);
				Transitions(tenumstateSource, tenumstateSink, pesetEvents);

				// loop through all connecting edges
				EsetEventsIter esetIterTrans(*pesetEvents);
				while (esetIterTrans.Advance())
				{
					// apply walker function
					Pfpv(this, tenumstateSource, tenumstateSink,
						 esetIterTrans.TBit(), pvContext);
				}

				pesetEvents->Release();
			}
		}

		pesetStates->Release();
	}

	// print function -- used with walker
	static void
	Diagram(const CStateMachine *psm, TEnumState tenumstateSource,
			TEnumState tenumstateSink, TEnumEvent tenumevent, void *pvContext)
	{
		IOstream &os = *(IOstream *) pvContext;

		os << "\"" << psm->WszState(tenumstateSource) << "\" -> \""
		   << psm->WszState(tenumstateSink) << "\" [ label = \""
		   << psm->WszEvent(tenumevent) << "\" ];" << std::endl;
	}


	// check for unreachable nodes -- used with walker
	static void
	Unreachable(const CStateMachine *, TEnumState tenumstateSource,
				TEnumState tenumstateSink, TEnumEvent, void *pvContext)
	{
		EsetStates &eset = *(EsetStates *) pvContext;

		if (tenumstateSource != tenumstateSink)
		{
			// reachable -- remove from set of unreachables
			(void) eset.ExchangeClear(tenumstateSink);
		}
	}
#endif	// GPOS_DEBUG

	// actual implementation of transition
	BOOL
	FAttemptTransition(TEnumState tenumstateOld, TEnumEvent tenumevent,
					   TEnumState &tenumstateNew) const
	{
		GPOS_ASSERT(tenumevent < tenumeventSentinel);
		GPOS_ASSERT(m_fInit);

		for (ULONG ulOuter = 0; ulOuter < tenumstateSentinel; ulOuter++)
		{
			if (m_rgrgtenumeventTransitions[tenumstateOld][ulOuter] ==
				tenumevent)
			{
#ifdef GPOS_DEBUG
				// make sure there isn't another transition possible for the same event
				for (ULONG ulInner = ulOuter + 1; ulInner < tenumstateSentinel;
					 ulInner++)
				{
					GPOS_ASSERT(
						m_rgrgtenumeventTransitions[tenumstateOld][ulInner] !=
						tenumevent);
				}
#endif	// GPOS_DEBUG

				tenumstateNew = (TEnumState) ulOuter;
				return true;
			}
		}

		return false;
	}

	// hidden copy ctor
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent,
				  tenumeventSentinel>(
		const CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent,
							tenumeventSentinel> &);

public:
	// ctor
	CStateMachine<TEnumState, tenumstateSentinel, TEnumEvent,
				  tenumeventSentinel>()
		: m_tenumstate(TesInitial()),
		  m_fInit(false)
#ifdef GPOS_DEBUG
		  ,
		  m_ulHistory(0)
#endif	// GPOS_DEBUG
	{
		GPOS_ASSERT(0 < tenumstateSentinel && 0 < tenumeventSentinel &&
					(ULONG) tenumeventSentinel + 1 >=
						(ULONG) tenumstateSentinel);
	}

	// initialize state machine
	void
	Init(const TEnumEvent rgrgtenumeventTransitions[tenumstateSentinel]
												   [tenumstateSentinel]
#ifdef GPOS_DEBUG
		 ,
		 const WCHAR rgwszStates[tenumstateSentinel][GPOPT_FSM_NAME_LENGTH],
		 const WCHAR rgwszEvents[tenumeventSentinel][GPOPT_FSM_NAME_LENGTH]
#endif	// GPOS_DEBUG
	)
	{
		GPOS_ASSERT(!m_fInit);

		for (ULONG ulOuter = 0; ulOuter < tenumstateSentinel; ulOuter++)
		{
			for (ULONG ulInner = 0; ulInner < tenumstateSentinel; ulInner++)
			{
				m_rgrgtenumeventTransitions[ulOuter][ulInner] =
					rgrgtenumeventTransitions[ulOuter][ulInner];
			}
		}

#ifdef GPOS_DEBUG
		for (ULONG ul = 0; ul < tenumstateSentinel; ul++)
		{
			m_rgwszStates[ul] = rgwszStates[ul];
		}

		for (ULONG ul = 0; ul < tenumeventSentinel; ul++)
		{
			m_rgwszEvents[ul] = rgwszEvents[ul];
		}
#endif	// GPOS_DEBUG

		m_tenumstate = TesInitial();
		m_fInit = true;
	}

	// dtor
	~CStateMachine(){};

	// attempt transition
	BOOL
	FTransition(TEnumEvent tenumevent, TEnumState &tenumstate)
	{
		TEnumState tenumstateNew;
		BOOL fSucceeded =
			FAttemptTransition(m_tenumstate, tenumevent, tenumstateNew);

		if (fSucceeded)
		{
			m_tenumstate = tenumstateNew;
#ifdef GPOS_DEBUG
			RecordHistory(tenumevent, m_tenumstate);
#endif	// GPOS_DEBUG
		}

		tenumstate = m_tenumstate;
		return fSucceeded;
	}

	// shorthand if current state and return value are not needed
	void
	Transition(TEnumEvent tenumevent)
	{
		TEnumState tenumstateDummy;
#ifdef GPOS_DEBUG
		BOOL fCheck =
#else
		(void)
#endif	// GPOS_DEBUG
			FTransition(tenumevent, tenumstateDummy);

		GPOS_ASSERT(fCheck && "Event rejected");
	}

	// inspect current state; to be used only in assertions
	TEnumState
	Estate() const
	{
		return m_tenumstate;
	}

	// get initial state
	TEnumState
	TesInitial() const
	{
		return (TEnumState) 0;
	}

	// get final state
	TEnumState
	TesFinal() const
	{
		return (TEnumState)(tenumstateSentinel - 1);
	}

	// reset state
	void
	Reset()
	{
		m_tenumstate = TesInitial();
		m_fInit = false;
	}

#ifdef GPOS_DEBUG
	// dump history
	IOstream &
	OsHistory(IOstream &os) const
	{
		ULONG ulElems = std::min(m_ulHistory, GPOPT_FSM_HISTORY);

		ULONG ulStart = m_ulHistory + 1;
		if (m_ulHistory < GPOPT_FSM_HISTORY)
		{
			// if we haven't rolled over, just start at 0
			ulStart = 0;
		}

		os << "State Machine History (" << (void *) this << ")" << std::endl;

		for (ULONG ul = 0; ul < ulElems; ul++)
		{
			ULONG ulPos = (ulStart + ul) % GPOPT_FSM_HISTORY;
			os << ul << ": " << WszEvent(m_tenumeventHistory[ulPos])
			   << " (event) -> " << WszState(m_tenumstateHistory[ulPos])
			   << " (state)" << std::endl;
		}

		return os;
	}

	// check for unreachable states
	BOOL
	FReachable(CMemoryPool *mp) const
	{
		TEnumState *pestate = NULL;
		ULONG size = 0;
		Unreachable(mp, &pestate, &size);
		GPOS_DELETE_ARRAY(pestate);

		return (size == 0);
	}

	// compute array of unreachable states
	void
	Unreachable(CMemoryPool *mp, TEnumState **ppestate, ULONG *pulSize) const
	{
		GPOS_ASSERT(NULL != ppestate);
		GPOS_ASSERT(NULL != pulSize);

		// initialize output array
		*ppestate = GPOS_NEW_ARRAY(mp, TEnumState, tenumstateSentinel);
		for (ULONG ul = 0; ul < tenumstateSentinel; ul++)
		{
			(*ppestate)[ul] = tenumstateSentinel;
		}

		// mark all states unreachable at first
		EsetStates *peset = GPOS_NEW(mp) EsetStates(mp);
		States(peset);

		Walk(mp, Unreachable, peset);

		// store remaining states in output array
		EsetStatesIter esetIter(*peset);
		ULONG ul = 0;
		while (esetIter.Advance())
		{
			(*ppestate)[ul++] = esetIter.TBit();
		}
		*pulSize = ul;
		peset->Release();
	}

	// dump Moore diagram in graphviz format
	IOstream &
	OsDiagramToGraphviz(CMemoryPool *mp, IOstream &os,
						const WCHAR *wszTitle) const
	{
		os << "digraph " << wszTitle << " { " << std::endl
		   << GRAPHVIZ_DOUBLE_CIRCLE(WszState(TesInitial())) << std::endl;

		// get unreachable states
		EsetStates *peset = GPOS_NEW(mp) EsetStates(mp);
		States(peset);

		Walk(mp, Unreachable, peset);

		// print all unreachable nodes using BOXes
		EsetStatesIter esetIter(*peset);
		while (esetIter.Advance())
		{
			os << GRAPHVIZ_BOX(WszState(esetIter.TBit())) << std::endl;
		}
		peset->Release();

		// print the remainder of the diagram by writing all edges only;
		// nodes are implicit;
		Walk(mp, Diagram, &os);

		os << "} " << std::endl;
		return os;
	}

#endif	// GPOS_DEBUG

};	// class CStateMachine
}  // namespace gpopt

#endif