//---------------------------------------------------------------------------
//	@filename:
//		IBucket.h
//
//	@doc:
//		Simple bucket interface
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IBucket_H
#define GPNAUCRATES_IBucket_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/statistics/CPoint.h"

namespace gpnaucrates
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		IBucket
//
//	@doc:
//		Simple bucket interface. Has a lower point and upper point
//
//---------------------------------------------------------------------------

class IBucket
{
private:
	// private copy constructor
	IBucket(const IBucket &);

	// private assignment operator
	IBucket &operator=(const IBucket &);

public:
	// c'tor
	IBucket()
	{
	}

	// lower point
	virtual CPoint *GetLowerBound() const = 0;

	// upper point
	virtual CPoint *GetUpperBound() const = 0;

	// is bucket singleton?
	BOOL
	IsSingleton() const
	{
		return GetLowerBound()->Equals(GetUpperBound());
	}

	// d'tor
	virtual ~IBucket()
	{
	}
};
}  // namespace gpnaucrates

#endif
