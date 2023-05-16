//---------------------------------------------------------------------------
//	@filename:
//		IMDColStats.h
//
//	@doc:
//		Interface for column stats
//---------------------------------------------------------------------------
#ifndef GPMD_IMDColStats_H
#define GPMD_IMDColStats_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/IMDCacheObject.h"
#include "duckdb/optimizer/cascade/common/CDouble.h"

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDColStats
//
//	@doc:
//		Interface for column stats
//
//---------------------------------------------------------------------------
class IMDColStats : public IMDCacheObject
{
public:
	// object type
	virtual Emdtype
	MDType() const
	{
		return EmdtColStats;
	}

	// number of buckets
	virtual ULONG Buckets() const = 0;

	// width
	virtual CDouble Width() const = 0;

	// null fraction
	virtual CDouble GetNullFreq() const = 0;

	// ndistinct of remaining tuples
	virtual CDouble GetDistinctRemain() const = 0;

	// frequency of remaining tuples
	virtual CDouble GetFreqRemain() const = 0;

	// is the columns statistics missing in the database
	virtual BOOL IsColStatsMissing() const = 0;
};
}  // namespace gpmd

#endif