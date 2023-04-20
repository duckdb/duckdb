//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2PartialDynamicIndexGetApply.h
//
//	@doc:
//		Transform inner join over partitioned table into a union-all of dynamic index get applies.
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoin2PartialDynamicIndexGetApply_H
#define GPOPT_CXformInnerJoin2PartialDynamicIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApply.h"
#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

// fwd declarations
class CExpression;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoin2PartialDynamicIndexGetApply
//
//	@doc:
//		Transform inner join over partitioned table into a union-all of
//		dynamic index get applies.
//
//---------------------------------------------------------------------------
class CXformInnerJoin2PartialDynamicIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalInnerJoin, CLogicalIndexApply, CLogicalDynamicGet,
		  false /*fWithSelect*/, true /*is_partial*/, IMDIndex::EmdindBtree>
{
public:
	// ctor
	explicit CXformInnerJoin2PartialDynamicIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalInnerJoin, CLogicalIndexApply,
									CLogicalDynamicGet, false /*fWithSelect*/,
									true /*is_partial*/, IMDIndex::EmdindBtree>(
			  mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoin2PartialDynamicIndexGetApply()
	{
	}

	// compute xform promise for a given expression handle
	virtual CXform::EXformPromise
	Exfp(CExpressionHandle &exprhdl) const
	{
		if (CXform::ExfpNone == CXformJoin2IndexApply::Exfp(exprhdl))
		{
			return CXform::ExfpNone;
		}

		if (exprhdl.DeriveHasPartialIndexes(1))
		{
			return CXform::ExfpHigh;
		}

		return CXform::ExfpNone;
	}

	// ident accessor
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoin2PartialDynamicIndexGetApply;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoin2PartialDynamicIndexGetApply";
	}

	// return true if xform should be applied only once
	virtual BOOL
	IsApplyOnce()
	{
		return true;
	}
};
}  // namespace gpopt

#endif	// !GPOPT_CXformInnerJoin2PartialDynamicIndexGetApply_H

// EOF
