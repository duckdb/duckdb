//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpression.cpp
//
//	@doc:
//		Implementation of group expression job superclass
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExpression.h"

#include "duckdb/optimizer/cascade/operators/CLogical.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpression::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void gpopt::CJobGroupExpression::Init(gpopt::CGroupExpression *pgexpr)
{
	GPOS_ASSERT(!FInit());
	GPOS_ASSERT(NULL != pgexpr);
	GPOS_ASSERT(NULL != pgexpr->Pgroup());
	GPOS_ASSERT(NULL != pgexpr->Pop());

	m_fChildrenScheduled = false;
	m_fXformsScheduled = false;
	m_pgexpr = pgexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpression::ScheduleTransformations
//
//	@doc:
//		Schedule transformation jobs for the given set of xforms
//
//---------------------------------------------------------------------------
void gpopt::CJobGroupExpression::ScheduleTransformations(gpopt::CSchedulerContext *psc, gpopt::CXformSet *xform_set)
{
	// iterate on xforms
	gpopt::CXformSetIter xsi(*(xform_set));
	while (xsi.Advance())
	{
		gpopt::CXform *pxform = CXformFactory::Pxff()->Pxf(xsi.TBit());
		CJobTransformation::ScheduleJob(psc, m_pgexpr, pxform, this);
	}
}