//---------------------------------------------------------------------------
//	@filename:
//		IMDAggregate.h
//
//	@doc:
//		Interface for aggregates in the metadata cache
//---------------------------------------------------------------------------
#ifndef GPMD_IMDAggregate_H
#define GPMD_IMDAggregate_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/IMDCacheObject.h"

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDAggregate
//
//	@doc:
//		Interface for aggregates in the metadata cache
//
//---------------------------------------------------------------------------
class IMDAggregate : public IMDCacheObject
{
public:
	// object type
	virtual Emdtype
	MDType() const
	{
		return EmdtAgg;
	}

	// type of intermediate results computed by the aggregate's
	// transformation function
	virtual IMDId *GetIntermediateResultTypeMdid() const = 0;

	// result type
	virtual IMDId *GetResultTypeMdid() const = 0;

	// is aggregate ordered
	virtual BOOL IsOrdered() const = 0;

	// is aggregate splittable
	virtual BOOL IsSplittable() const = 0;

	virtual
		// is aggregate hash capable
		BOOL
		IsHashAggCapable() const = 0;
};
}  // namespace gpmd

#endif