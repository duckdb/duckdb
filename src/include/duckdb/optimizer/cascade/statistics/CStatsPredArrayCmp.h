//---------------------------------------------------------------------------
//	@filename:
//		CStatsPredArrayCmp.h
//
//	@doc:
//		ArrayCmp filter on statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredArrayCmp_H
#define GPNAUCRATES_CStatsPredArrayCmp_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/statistics/CPoint.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPred.h"

// fwd declarations
namespace gpopt
{
class CColRef;
}

namespace gpnaucrates
{
using namespace gpos;
using namespace gpmd;
using namespace gpopt;

class CStatsPredArrayCmp : public CStatsPred
{
private:
	// private copy ctor
	CStatsPredArrayCmp(const CStatsPredArrayCmp &);

	// private assignment operator
	CStatsPredArrayCmp &operator=(CStatsPredArrayCmp &);

	// comparison type
	CStatsPred::EStatsCmpType m_stats_cmp_type;

	CPointArray *m_points;

public:
	// ctor
	CStatsPredArrayCmp(ULONG colid, CStatsPred::EStatsCmpType stats_cmp_type,
					   CPointArray *points);

	// dtor
	virtual ~CStatsPredArrayCmp()
	{
		m_points->Release();
	}

	// comparison types for stats computation
	virtual CStatsPred::EStatsCmpType
	GetCmpType() const
	{
		return m_stats_cmp_type;
	}

	// filter type id
	virtual EStatsPredType
	GetPredStatsType() const
	{
		return CStatsPred::EsptArrayCmp;
	}

	virtual CPointArray *
	GetPoints() const
	{
		return m_points;
	}

	// conversion function
	static CStatsPredArrayCmp *
	ConvertPredStats(CStatsPred *pred_stats)
	{
		GPOS_ASSERT(NULL != pred_stats);
		GPOS_ASSERT(CStatsPred::EsptArrayCmp == pred_stats->GetPredStatsType());

		return dynamic_cast<CStatsPredArrayCmp *>(pred_stats);
	}

};	// class CStatsPredArrayCmp

}  // namespace gpnaucrates

#endif
