//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPred.h
//
//	@doc:
//		Filter on statistics
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPred_H
#define GPNAUCRATES_CStatsPred_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

namespace gpnaucrates
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPred
//
//	@doc:
//		Filter on statistics
//---------------------------------------------------------------------------
class CStatsPred : public CRefCount
{
public:
	enum EStatsPredType
	{
		EsptPoint,		  // filter with literals
		EsptArrayCmp,	  // filter with = ANY/ALL(ARRAY[...])
		EsptConj,		  // conjunctive filter
		EsptDisj,		  // disjunctive filter
		EsptLike,		  // LIKE filter
		EsptUnsupported,  // unsupported filter for statistics calculation

		EsptSentinel
	};

	// comparison types for stats computation
	enum EStatsCmpType
	{
		EstatscmptEq,		// equals
		EstatscmptNEq,		// not equals
		EstatscmptL,		// less than
		EstatscmptLEq,		// less or equal to
		EstatscmptG,		// greater than
		EstatscmptGEq,		// greater or equal to
		EstatscmptIDF,		// is distinct from
		EstatscmptINDF,		// is not distinct from
		EstatscmptLike,		// LIKE predicate comparison
		EstatscmptNotLike,	// NOT LIKE predicate comparison
		// NDV comparison for equality predicate on columns with functions, ex f(a) = b or a = f(b)
		EstatscmptEqNDV,
		EstatscmptOther
	};

private:
	// private copy ctor
	CStatsPred(const CStatsPred &);

	// private assignment operator
	CStatsPred &operator=(CStatsPred &);

protected:
	// column id
	ULONG m_colid;

public:
	// ctor
	explicit CStatsPred(ULONG colid) : m_colid(colid)
	{
	}

	// dtor
	virtual ~CStatsPred()
	{
	}

	// accessors
	virtual ULONG
	GetColId() const
	{
		return m_colid;
	}

	// type id
	virtual EStatsPredType GetPredStatsType() const = 0;

	// comparison function
	static inline INT StatsPredSortCmpFunc(const void *val1, const void *val2);
};	// class CStatsPred

// array of filters
typedef CDynamicPtrArray<CStatsPred, CleanupRelease> CStatsPredPtrArry;

// comparison function for sorting predicates
INT
CStatsPred::StatsPredSortCmpFunc(const void *val1, const void *val2)
{
	const CStatsPred *stats_pred1 = *(const CStatsPred **) val1;
	const CStatsPred *stats_pred2 = *(const CStatsPred **) val2;

	return (INT) stats_pred1->GetColId() - (INT) stats_pred2->GetColId();
}

}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CStatsPred_H

// EOF
