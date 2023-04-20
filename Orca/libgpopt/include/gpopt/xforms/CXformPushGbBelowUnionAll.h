//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformPushGbBelowUnionAll.h
//
//	@doc:
//		Push grouping below UnionAll operation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushGbBelowUnionAll_H
#define GPOPT_CXformPushGbBelowUnionAll_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformPushGbBelowSetOp.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushGbBelowUnionAll
//
//	@doc:
//		Push grouping below UnionAll operation
//
//---------------------------------------------------------------------------
class CXformPushGbBelowUnionAll
	: public CXformPushGbBelowSetOp<CLogicalUnionAll>
{
private:
	// private copy ctor
	CXformPushGbBelowUnionAll(const CXformPushGbBelowUnionAll &);

public:
	// ctor
	explicit CXformPushGbBelowUnionAll(CMemoryPool *mp)
		: CXformPushGbBelowSetOp<CLogicalUnionAll>(mp)
	{
	}

	// dtor
	virtual ~CXformPushGbBelowUnionAll()
	{
	}

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfPushGbBelowUnionAll != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfPushGbBelowUnionAll;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformPushGbBelowUnionAll";
	}

};	// class CXformPushGbBelowUnionAll

}  // namespace gpopt

#endif	// !GPOPT_CXformPushGbBelowUnionAll_H

// EOF
