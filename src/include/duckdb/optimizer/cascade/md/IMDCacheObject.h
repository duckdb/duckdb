//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		IMDCacheObject.h
//
//	@doc:
//		Base interface for metadata cache objects
//---------------------------------------------------------------------------
#ifndef GPMD_IMDCacheObject_H
#define GPMD_IMDCacheObject_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/md/CMDName.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"
#include "duckdb/optimizer/cascade/md/IMDInterface.h"


// fwd decl
namespace gpos
{
class CWStringDynamic;
}

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDCacheObject
//
//	@doc:
//		Base interface for metadata cache objects
//
//---------------------------------------------------------------------------
class IMDCacheObject : public IMDInterface
{
public:
	// type of md object
	enum Emdtype
	{
		EmdtRel,
		EmdtInd,
		EmdtFunc,
		EmdtAgg,
		EmdtOp,
		EmdtType,
		EmdtTrigger,
		EmdtCheckConstraint,
		EmdtRelStats,
		EmdtColStats,
		EmdtCastFunc,
		EmdtScCmp
	};

	// md id of cache object
	virtual IMDId *MDId() const = 0;

	// cache object name
	virtual CMDName Mdname() const = 0;

	// object type
	virtual Emdtype MDType() const = 0;

	// DXL string representation of cache object
	virtual const CWStringDynamic *GetStrRepr() const = 0;

#ifdef GPOS_DEBUG
	virtual void DebugPrint(IOstream &os) const = 0;
#endif
};

typedef CDynamicPtrArray<IMDCacheObject, CleanupRelease> IMDCacheObjectArray;

}  // namespace gpmd



#endif
