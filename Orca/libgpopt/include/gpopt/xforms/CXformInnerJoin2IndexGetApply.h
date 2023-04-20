//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2IndexGetApply.h
//
//	@doc:
//		Transform Inner Join to IndexGet Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2IndexGetApply_H
#define GPOPT_CXformInnerJoin2IndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoin2IndexGetApply
//
//	@doc:
//		Transform Inner Join to IndexGet Apply
//
//---------------------------------------------------------------------------
class CXformInnerJoin2IndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalInnerJoin, CLogicalIndexApply, CLogicalGet,
		  false /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBtree>
{
private:
	// private copy ctor
	CXformInnerJoin2IndexGetApply(const CXformInnerJoin2IndexGetApply &);

public:
	// ctor
	explicit CXformInnerJoin2IndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalInnerJoin, CLogicalIndexApply,
									CLogicalGet, false /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBtree>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoin2IndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoin2IndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoin2IndexGetApply";
	}

};	// class CXformInnerJoin2IndexGetApply

}  // namespace gpopt

#endif	// !GPOPT_CXformInnerJoin2IndexGetApply_H

// EOF
