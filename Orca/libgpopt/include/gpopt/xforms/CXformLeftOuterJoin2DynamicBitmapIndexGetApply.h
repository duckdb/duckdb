//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2019 Pivotal, Inc.
//
//	Transform Left Outer Join to Bitmap IndexGet Apply for a partitioned table
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformLeftOuterJoin2DynamicBitmapIndexGetApply_H
#define GPOPT_CXformLeftOuterJoin2DynamicBitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuterJoin2DynamicBitmapIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalLeftOuterJoin, CLogicalIndexApply, CLogicalDynamicGet,
		  false /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBitmap>
{
private:
	// private copy ctor
	CXformLeftOuterJoin2DynamicBitmapIndexGetApply(
		const CXformLeftOuterJoin2DynamicBitmapIndexGetApply &);

public:
	// ctor
	explicit CXformLeftOuterJoin2DynamicBitmapIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalLeftOuterJoin, CLogicalIndexApply,
									CLogicalDynamicGet, false /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBitmap>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterJoin2DynamicBitmapIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoin2DynamicBitmapIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoin2DynamicBitmapIndexGetApply";
	}
};	// class CXformLeftOuterJoin2DynamicBitmapIndexGetApply
}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuterJoin2DynamicBitmapIndexGetApply_H

// EOF
