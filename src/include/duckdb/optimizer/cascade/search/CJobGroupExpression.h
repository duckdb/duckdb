//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpression.h
//
//	@doc:
//		Superclass of group expression jobs
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupExpression_H
#define GPOPT_CJobGroupExpression_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

namespace gpopt
{
// prototypes
class gpopt::CGroup;
class gpopt::CGroupExpression;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExpression
//
//	@doc:
//		Abstract superclass of all group expression optimization jobs
//
//---------------------------------------------------------------------------
class CJobGroupExpression : public gpopt::CJob
{
private:
	// true if job has scheduled child group jobs
	gpos::BOOL m_fChildrenScheduled;

	// true if job has scheduled transformation jobs
	gpos::BOOL m_fXformsScheduled;

	// private copy ctor
	CJobGroupExpression(const CJobGroupExpression &);

protected:
	// target group expression
	gpopt::CGroupExpression *m_pgexpr;

	// ctor
	CJobGroupExpression() : m_pgexpr(NULL)
	{
	}

	// dtor
	virtual ~CJobGroupExpression()
	{
	}

	// has job scheduled child groups ?
	gpos::BOOL FChildrenScheduled() const
	{
		return m_fChildrenScheduled;
	}

	// set children scheduled
	void SetChildrenScheduled()
	{
		m_fChildrenScheduled = true;
	}

	// has job scheduled xform groups ?
	gpos::BOOL FXformsScheduled() const
	{
		return m_fXformsScheduled;
	}

	// set xforms scheduled
	void SetXformsScheduled()
	{
		m_fXformsScheduled = true;
	}

	// initialize job
	void Init(gpopt::CGroupExpression *pgexpr);

	// schedule transformation jobs for applicable xforms
	virtual void ScheduleApplicableTransformations(CSchedulerContext *psc) = 0;

	// schedule jobs for all child groups
	virtual void ScheduleChildGroupsJobs(CSchedulerContext *psc) = 0;

	// schedule transformation jobs for the given set of xforms
	void ScheduleTransformations(CSchedulerContext *psc, CXformSet *xform_set);

	// job's function
	virtual gpos::BOOL FExecute(CSchedulerContext *psc) = 0;

#ifdef GPOS_DEBUG

	// print function
	virtual gpos::IOstream &OsPrint(IOstream &os) = 0;

#endif	// GPOS_DEBUG

};	// class CJobGroupExpression

}  // namespace gpopt

#endif