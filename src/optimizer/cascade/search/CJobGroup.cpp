//---------------------------------------------------------------------------
//	@filename:
//		CJobGroup.cpp
//
//	@doc:
//		Implementation of group job superclass
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroup.h"

#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CJobGroup::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void
CJobGroup::Init(CGroup *pgroup)
{
	GPOS_ASSERT(!FInit());
	GPOS_ASSERT(NULL != pgroup);

	m_pgroup = pgroup;
	m_pgexprLastScheduled = NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroup::PgexprFirstUnschedNonLogical
//
//	@doc:
//		Get first non-logical group expression with an unscheduled job
//
//---------------------------------------------------------------------------
CGroupExpression *
CJobGroup::PgexprFirstUnschedNonLogical()
{
	CGroupExpression *pgexpr = NULL;
	{
		CGroupProxy gp(m_pgroup);
		if (NULL == m_pgexprLastScheduled)
		{
			// get first group expression
			pgexpr = gp.PgexprSkipLogical(NULL /*pgexpr*/);
		}
		else
		{
			// get group expression next to last scheduled one
			pgexpr = gp.PgexprSkipLogical(m_pgexprLastScheduled);
		}
	}

	return pgexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroup::PgexprFirstUnschedLogical
//
//	@doc:
//		Get first logical group expression with an unscheduled job
//
//---------------------------------------------------------------------------
CGroupExpression *
CJobGroup::PgexprFirstUnschedLogical()
{
	CGroupExpression *pgexpr = NULL;
	{
		CGroupProxy gp(m_pgroup);
		if (NULL == m_pgexprLastScheduled)
		{
			// get first group expression
			pgexpr = gp.PgexprFirst();
		}
		else
		{
			// get group expression next to last scheduled one
			pgexpr = gp.PgexprNext(m_pgexprLastScheduled);
		}
	}

	return pgexpr;
}