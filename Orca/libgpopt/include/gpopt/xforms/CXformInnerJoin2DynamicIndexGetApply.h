//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2DynamicIndexGetApply.h
//
//	@doc:
//		Transform Inner Join to DynamicIndexGet-Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2DynamicIndexGetApply_H
#define GPOPT_CXformInnerJoin2DynamicIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoin2DynamicIndexGetApply
//
//	@doc:
//		Transform Inner Join to DynamicIndexGet-Apply
//
//---------------------------------------------------------------------------
class CXformInnerJoin2DynamicIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalInnerJoin, CLogicalIndexApply, CLogicalDynamicGet,
		  false /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBtree>
{
private:
	// private copy ctor
	CXformInnerJoin2DynamicIndexGetApply(
		const CXformInnerJoin2DynamicIndexGetApply &);

public:
	// ctor
	explicit CXformInnerJoin2DynamicIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalInnerJoin, CLogicalIndexApply,
									CLogicalDynamicGet, false /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBtree>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoin2DynamicIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoin2DynamicIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoin2DynamicIndexGetApply";
	}

};	// class CXformInnerJoin2DynamicIndexGetApply

}  // namespace gpopt

#endif	// !GPOPT_CXformInnerJoin2DynamicIndexGetApply_H

// EOF
