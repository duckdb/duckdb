//---------------------------------------------------------------------------
//	@filename:
//		CSearchStage.h
//
//	@doc:
//		Search stage
//---------------------------------------------------------------------------
#ifndef CSearchStage_H
#define CSearchStage_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CTimerUser.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

using namespace gpos;

namespace gpopt
{
// forward declarations
class CSearchStage;

//---------------------------------------------------------------------------
//	@class:
//		CSearchStage
//
//	@doc:
//		Search stage
//
//---------------------------------------------------------------------------
class CSearchStage
{
public:
	// set of xforms to be applied during stage
	CXformSet* m_xforms;

	// time threshold in milliseconds
	ULONG m_time_threshold;

	// cost threshold
	double m_cost_threshold;

	// best plan found at the end of search stage
	duckdb::unique_ptr<Operator> m_pexprBest;

	// cost of best plan found
	double m_costBest;

	// elapsed time
	CTimerUser m_timer;

public:
	// ctor
	CSearchStage(CXformSet* xform_set, ULONG ulTimeThreshold = gpos::ulong_max, double costThreshold = 0.0);

	// dtor
	virtual ~CSearchStage();

	// restart timer if time threshold is not default indicating don't timeout
	// Restart() is a costly method, so avoid calling unnecessarily
	void RestartTimer()
	{
		if (m_time_threshold != gpos::ulong_max)
			m_timer.Restart();
	}

	// is search stage timed-out?
	// if threshold is gpos::ulong_max, its the default and we need not time out
	// ElapsedMS() is a costly method, so avoid calling unnecesarily
	bool FTimedOut() const
	{
		if (m_time_threshold == gpos::ulong_max)
			return false;
		return m_timer.ElapsedMS() > m_time_threshold;
	}

	// return elapsed time (in millseconds) since timer was last restarted
	ULONG UlElapsedTime() const
	{
		return m_timer.ElapsedMS();
	}

	bool FAchievedReqdCost() const
	{
		return (nullptr != m_pexprBest && m_costBest <= m_cost_threshold);
	}

	// set best plan found at the end of search stage
	void SetBestExpr(Operator* pexpr);

	// generate default search strategy
	static duckdb::vector<CSearchStage*> PdrgpssDefault();
};
}  // namespace gpopt
#endif