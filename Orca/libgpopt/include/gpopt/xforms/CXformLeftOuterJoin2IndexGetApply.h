//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform left outer Join to IndexGet Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterJoin2IndexGetApply_H
#define GPOPT_CXformLeftOuterJoin2IndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuterJoin2IndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalLeftOuterJoin, CLogicalIndexApply, CLogicalGet,
		  false /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBtree>
{
private:
	// private copy ctor
	CXformLeftOuterJoin2IndexGetApply(
		const CXformLeftOuterJoin2IndexGetApply &);

public:
	// ctor
	explicit CXformLeftOuterJoin2IndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalLeftOuterJoin, CLogicalIndexApply,
									CLogicalGet, false /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBtree>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterJoin2IndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoin2IndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoin2IndexGetApply";
	}

};	// class CXformLeftOuterJoin2IndexGetApply

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuterJoin2IndexGetApply_H

// EOF
