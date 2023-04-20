//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2BitmapIndexGetApply.h
//
//	@doc:
//		Transform Inner Join to Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoin2BitmapIndexGetApply_H
#define GPOPT_CXformInnerJoin2BitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoin2BitmapIndexGetApply
//
//	@doc:
//		Transform Inner Join to Bitmap IndexGet Apply
//
//---------------------------------------------------------------------------
class CXformInnerJoin2BitmapIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalInnerJoin, CLogicalIndexApply, CLogicalGet,
		  false /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBitmap>
{
private:
	// private copy ctor
	CXformInnerJoin2BitmapIndexGetApply(
		const CXformInnerJoin2BitmapIndexGetApply &);

public:
	// ctor
	explicit CXformInnerJoin2BitmapIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalInnerJoin, CLogicalIndexApply,
									CLogicalGet, false /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBitmap>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoin2BitmapIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoin2BitmapIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoin2BitmapIndexGetApply";
	}

};	// class CXformInnerJoin2BitmapIndexGetApply
}  // namespace gpopt

#endif	// !GPOPT_CXformInnerJoin2BitmapIndexGetApply_H

// EOF
