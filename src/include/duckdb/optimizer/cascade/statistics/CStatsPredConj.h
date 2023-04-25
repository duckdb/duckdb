//---------------------------------------------------------------------------
//	@filename:
//		CStatsPredConj.h
//
//	@doc:
//		Conjunctive filter on statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredConj_H
#define GPNAUCRATES_CStatsPredConj_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/statistics/CPoint.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPred.h"

namespace gpnaucrates
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPredConj
//
//	@doc:
//		Conjunctive filter on statistics
//---------------------------------------------------------------------------
class CStatsPredConj : public CStatsPred
{
private:
	// private copy ctor
	CStatsPredConj(const CStatsPredConj &);

	// private assignment operator
	CStatsPredConj &operator=(CStatsPredConj &);

	// array of filters
	CStatsPredPtrArry *m_conj_pred_stats_array;

public:
	// ctor
	explicit CStatsPredConj(CStatsPredPtrArry *pdrgpstatspred);

	// dtor
	virtual ~CStatsPredConj()
	{
		m_conj_pred_stats_array->Release();
	}

	// the column identifier on which the predicates are on
	virtual ULONG GetColId() const;

	// total number of predicates in the conjunction
	ULONG
	GetNumPreds() const
	{
		return m_conj_pred_stats_array->Size();
	}

	CStatsPredPtrArry *
	GetConjPredStatsArray() const
	{
		return m_conj_pred_stats_array;
	}

	// sort the components of the conjunction
	void Sort() const;

	// return the filter at a particular position
	CStatsPred *GetPredStats(ULONG pos) const;

	// filter type id
	virtual EStatsPredType
	GetPredStatsType() const
	{
		return CStatsPred::EsptConj;
	}

	// conversion function
	static CStatsPredConj *
	ConvertPredStats(CStatsPred *pred_stats)
	{
		GPOS_ASSERT(NULL != pred_stats);
		GPOS_ASSERT(CStatsPred::EsptConj == pred_stats->GetPredStatsType());

		return dynamic_cast<CStatsPredConj *>(pred_stats);
	}

};	// class CStatsPredConj
}  // namespace gpnaucrates

#endif
