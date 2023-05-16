//---------------------------------------------------------------------------
//	@filename:
//		IMDRelStats.h
//
//	@doc:
//		Interface for relation stats
//---------------------------------------------------------------------------
#ifndef GPMD_IMDRelStats_H
#define GPMD_IMDRelStats_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDouble.h"
#include "duckdb/optimizer/cascade/md/IMDCacheObject.h"

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDRelStats
//
//	@doc:
//		Interface for relation stats
//
//---------------------------------------------------------------------------
class IMDRelStats : public IMDCacheObject
{
public:
	// object type
	virtual Emdtype
	MDType() const
	{
		return EmdtRelStats;
	}

	// number of rows
	virtual CDouble Rows() const = 0;

	// is statistics on an empty input
	virtual BOOL IsEmpty() const = 0;
};
}  // namespace gpmd

#endif