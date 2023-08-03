//---------------------------------------------------------------------------
//	@filename:
//		CJobGroup.h
//
//	@doc:
//		Superclass of group jobs
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroup_H
#define GPOPT_CJobGroup_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJob.h"

using namespace gpos;

namespace gpopt
{
// prototypes
class CGroup;
class CGroupExpression;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroup
//
//	@doc:
//		Abstract superclass of all group optimization jobs
//
//---------------------------------------------------------------------------
class CJobGroup : public CJob
{
public:
	// target group
	CGroup* m_pgroup;

	// last scheduled group expression
	list<CGroupExpression*>::iterator m_pgexprLastScheduled;

	// ctor
	CJobGroup()
		: m_pgroup(NULL)
	{
	}

	// no copy ctor
	CJobGroup(const CJobGroup &) = delete;

	// dtor
	virtual ~CJobGroup(){};

	// initialize job
	void Init(CGroup* pgroup);

	// get first unscheduled logical expression
	virtual list<CGroupExpression*>::iterator PgexprFirstUnschedLogical();

	// get first unscheduled non-logical expression
	virtual list<CGroupExpression*>::iterator PgexprFirstUnschedNonLogical();

	// get first unscheduled expression
	virtual list<CGroupExpression*>::iterator PgexprFirstUnsched() = 0;

	// schedule jobs for of all new group expressions
	virtual bool FScheduleGroupExpressions(CSchedulerContext* psc) = 0;

	// job's function
	bool FExecute(CSchedulerContext* psc) override = 0;
};	// class CJobGroup
}  // namespace gpopt
#endif