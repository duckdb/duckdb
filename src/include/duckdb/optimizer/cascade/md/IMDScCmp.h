//---------------------------------------------------------------------------
//	@filename:
//		IMDScCmp.h
//
//	@doc:
//		Interface for scalar comparison operators in the MD cache
//---------------------------------------------------------------------------
#ifndef GPMD_IMDScCmp_H
#define GPMD_IMDScCmp_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/IMDCacheObject.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDScCmp
//
//	@doc:
//		Interface for scalar comparison operators in the MD cache
//
//---------------------------------------------------------------------------
class IMDScCmp : public IMDCacheObject
{
public:
	// object type
	virtual Emdtype
	MDType() const
	{
		return EmdtScCmp;
	}

	// left type
	virtual IMDId *GetLeftMdid() const = 0;

	// right type
	virtual IMDId *GetRightMdid() const = 0;

	// comparison type
	virtual IMDType::ECmpType ParseCmpType() const = 0;

	// comparison operator id
	virtual IMDId *MdIdOp() const = 0;
};

}  // namespace gpmd

#endif