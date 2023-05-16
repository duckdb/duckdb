//---------------------------------------------------------------------------
//	@filename:
//		CStatsPredJoin.h
//
//	@doc:
//		Join predicate used for join cardinality estimation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredJoin_H
#define GPNAUCRATES_CStatsPredJoin_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPred.h"

namespace gpnaucrates
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPredJoin
//
//	@doc:
//		Join predicate used for join cardinality estimation
//---------------------------------------------------------------------------
class CStatsPredJoin : public CRefCount
{
private:
	// private copy ctor
	CStatsPredJoin(const CStatsPredJoin &);

	// private assignment operator
	CStatsPredJoin &operator=(CStatsPredJoin &);

	// column id
	ULONG m_colidOuter;

	// comparison type
	CStatsPred::EStatsCmpType m_stats_cmp_type;

	// column id
	ULONG m_colidInner;

public:
	// c'tor
	CStatsPredJoin(ULONG colid1, CStatsPred::EStatsCmpType stats_cmp_type,
				   ULONG colid2)
		: m_colidOuter(colid1),
		  m_stats_cmp_type(stats_cmp_type),
		  m_colidInner(colid2)
	{
	}

	// accessors
	BOOL
	HasValidColIdOuter() const
	{
		return gpos::ulong_max != m_colidOuter;
	}

	ULONG
	ColIdOuter() const
	{
		return m_colidOuter;
	}

	// comparison type
	CStatsPred::EStatsCmpType
	GetCmpType() const
	{
		return m_stats_cmp_type;
	}

	BOOL
	HasValidColIdInner() const
	{
		return gpos::ulong_max != m_colidInner;
	}

	ULONG
	ColIdInner() const
	{
		return m_colidInner;
	}

	// d'tor
	virtual ~CStatsPredJoin()
	{
	}

};	// class CStatsPredJoin

// array of filters
typedef CDynamicPtrArray<CStatsPredJoin, CleanupRelease> CStatsPredJoinArray;
}  // namespace gpnaucrates

#endif