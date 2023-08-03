//---------------------------------------------------------------------------
//	@filename:
//		CJobTransformation.h
//
//	@doc:
//		Transform group expression job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobTransformation_H
#define GPOPT_CJobTransformation_H

#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

using namespace gpos;

namespace gpopt
{
// prototypes
class CGroup;
class CGroupExpression;
class CXform;

//---------------------------------------------------------------------------
//	@class:
//		CJobTransformation
//
//	@doc:
//		Runs the given transformation (XForm) rule.
//
//---------------------------------------------------------------------------
class CJobTransformation : public CJob
{
public:
	// transition events of a transformation
	enum EEvent
	{ eevCompleted, eevSentinel };

	// states of a transformation
	enum EState
	{ estInitialized = 0, estCompleted, estSentinel };

public:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// target group expression
	CGroupExpression* m_pgexpr;

	// xform to apply to group expression
	CXform* m_xform;

	// job state machine
	JSM m_jsm;

public:
	// ctor
	CJobTransformation();
	
	// private copy ctor
	CJobTransformation(const CJobTransformation &) = delete;
	
	// dtor
	virtual ~CJobTransformation();

public:
	// apply transformation action
	static EEvent EevtTransform(CSchedulerContext* psc, CJob* pj);

	// initialize job
	void Init(CGroupExpression* pgexpr, CXform* pxform);

	// schedule a new transformation job
	static void ScheduleJob(CSchedulerContext* psc, CGroupExpression* pgexpr, CXform* pxform, CJob* pjParent);

	// job's main function
	virtual bool FExecute(CSchedulerContext* psc);

	// conversion function
	static CJobTransformation* PjConvert(CJob* pj)
	{
		return dynamic_cast<CJobTransformation*>(pj);
	}
};	// class CJobTransformation
}  // namespace gpopt
#endif