//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInnerApplyWithOuterKey2InnerJoin.h
//
//	@doc:
//		Turn inner apply into inner join under the condition that
//		outer child of apply has key
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerApplyWithOuterKey2InnerJoin_H
#define GPOPT_CXformInnerApplyWithOuterKey2InnerJoin_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CDecorrelator.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerApplyWithOuterKey2InnerJoin
//
//	@doc:
//		Transform inner apply into inner join under the condition that
//		outer child of apply has key
//
//---------------------------------------------------------------------------
class CXformInnerApplyWithOuterKey2InnerJoin : public CXformExploration
{
private:
	// private copy ctor
	CXformInnerApplyWithOuterKey2InnerJoin(
		const CXformInnerApplyWithOuterKey2InnerJoin &);

public:
	// ctor
	explicit CXformInnerApplyWithOuterKey2InnerJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformInnerApplyWithOuterKey2InnerJoin()
	{
	}

	// transformation promise
	EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerApplyWithOuterKey2InnerJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerApplyWithOuterKey2InnerJoin";
	}

	// is transformation an Apply decorrelation (Apply To Join) xform?
	virtual BOOL
	FApplyDecorrelating() const
	{
		return true;
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;


};	// class CXformInnerApplyWithOuterKey2InnerJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformInnerApplyWithOuterKey2InnerJoin_H

// EOF
