//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupOptimization.h
//
//	@doc:
//		Optimize group job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupOptimization_H
#define GPOPT_CJobGroupOptimization_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

namespace gpopt
{
using namespace gpos;

// prototypes
class COptimizationContext;


//---------------------------------------------------------------------------
//	@class:
//		CJobGroupOptimization
//
//	@doc:
//		Group optimization job
//
//		Responsible for finding the best plan rooted by an expression in a
//		given group, such that the identified plan satisfies given required
//		plan properties
//
//---------------------------------------------------------------------------
class CJobGroupOptimization : public CJobGroup
{
public:
	// transition events of group optimization
	enum EEvent
	{
		eevImplementing, eevImplemented, eevOptimizing, eevOptimizedCurrentLevel, eevOptimized, eevSentinel
	};

	// states of group optimization job
	enum EState
	{
		estInitialized = 0, estOptimizingChildren, estDampingOptimizationLevel, estCompleted, estSentinel
	};

private:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

	// group expression that triggered group optimization
	CGroupExpression* m_pgexprOrigin;

	// optimization context of the job
	COptimizationContext* m_poc;

	// current optimization level of group expressions
	EOptimizationLevel m_eolCurrent;

	// start optimization action
	static EEvent EevtStartOptimization(CSchedulerContext* psc, CJob* pj);

	// optimized child group expressions action
	static EEvent EevtOptimizeChildren(CSchedulerContext* psc, CJob* pj);

	// complete optimization action
	static EEvent EevtCompleteOptimization(CSchedulerContext* psc, CJob* pj);

public:
	// ctor
	CJobGroupOptimization();

	// no copy ctor
	CJobGroupOptimization(const CJobGroupOptimization &) = delete;

	// dtor
	virtual ~CJobGroupOptimization();

	// initialize job
	void Init(CGroup* pgroup, CGroupExpression* pgexprOrigin, COptimizationContext* poc);

	// current optimization level accessor
	EOptimizationLevel EolCurrent() const
	{
		return m_eolCurrent;
	}

	// damp optimization level of member group expressions
	void DampOptimizationLevel()
	{
		m_eolCurrent = CEngine::EolDamp(m_eolCurrent);
	}

	// get first unscheduled expression
	virtual list<CGroupExpression*>::iterator PgexprFirstUnsched() override
	{
		return CJobGroup::PgexprFirstUnschedNonLogical();
	}

	// schedule optimization jobs for of all new group expressions
	virtual bool FScheduleGroupExpressions(CSchedulerContext* psc) override;

	// schedule a new group optimization job
	static void ScheduleJob(CSchedulerContext* psc, CGroup* pgroup, CGroupExpression* pgexprOrigin, COptimizationContext* poc, CJob* pjParent);

	// job's function
	virtual bool FExecute(CSchedulerContext* psc) override;

	// conversion function
	static CJobGroupOptimization* PjConvert(CJob* pj)
	{
		return dynamic_cast<CJobGroupOptimization*>(pj);
	}
};	// class CJobGroupOptimization
}  // namespace gpopt
#endif