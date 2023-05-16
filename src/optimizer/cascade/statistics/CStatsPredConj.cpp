//---------------------------------------------------------------------------
//	@filename:
//		CStatsPredConj.cpp
//
//	@doc:
//		Implementation of statistics Conjunctive filter
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CStatsPredConj.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredConj::CStatsPredConj
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredConj::CStatsPredConj(CStatsPredPtrArry *conj_pred_stats_array)
	: CStatsPred(gpos::ulong_max),
	  m_conj_pred_stats_array(conj_pred_stats_array)
{
	GPOS_ASSERT(NULL != conj_pred_stats_array);
	m_colid = CStatisticsUtils::GetColId(conj_pred_stats_array);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredConj::GetPredStats
//
//	@doc:
//		Return the filter at a particular position
//
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredConj::GetPredStats(ULONG pos) const
{
	return (*m_conj_pred_stats_array)[pos];
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredConj::Sort
//
//	@doc:
//		Sort the components of the disjunction
//
//---------------------------------------------------------------------------
void
CStatsPredConj::Sort() const
{
	if (1 < GetNumPreds())
	{
		// sort the filters on column ids
		m_conj_pred_stats_array->Sort(CStatsPred::StatsPredSortCmpFunc);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredConj::GetColId
//
//	@doc:
//		Return the column identifier on which the predicates are on
//
//---------------------------------------------------------------------------
ULONG
CStatsPredConj::GetColId() const
{
	return m_colid;
}