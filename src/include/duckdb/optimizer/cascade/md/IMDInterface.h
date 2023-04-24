//---------------------------------------------------------------------------
//	@filename:
//		IMDInterface.h
//
//	@doc:
//		Base interface for metadata-related objects
//---------------------------------------------------------------------------
#ifndef GPMD_IMDInterface_H
#define GPMD_IMDInterface_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDInterface
//
//	@doc:
//		Base interface for metadata-related objects
//
//---------------------------------------------------------------------------
class IMDInterface : public CRefCount
{
public:
	virtual ~IMDInterface()
	{
	}
};
}  // namespace gpmd

#endif
