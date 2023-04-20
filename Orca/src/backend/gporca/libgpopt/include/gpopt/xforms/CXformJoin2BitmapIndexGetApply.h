//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (c) 2020 VMware and affiliates, Inc.
//
// Transform a join into a bitmap index apply. Allow a variety of nodes on
// the inner side, including a mandatory get, plus optional select,
// project and aggregate nodes.
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoin2BitmapIndexGetApply_H
#define GPOPT_CXformJoin2BitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternNode.h"
#include "gpopt/xforms/CXformJoin2IndexApplyGeneric.h"

namespace gpopt
{
using namespace gpos;

class CXformJoin2BitmapIndexGetApply : public CXformJoin2IndexApplyGeneric
{
private:
public:
	CXformJoin2BitmapIndexGetApply(const CXformJoin2BitmapIndexGetApply &) =
		delete;

	// ctor
	explicit CXformJoin2BitmapIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyGeneric(mp, true)
	{
	}

	// dtor
	~CXformJoin2BitmapIndexGetApply() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfJoin2BitmapIndexGetApply;
	}

	const CHAR *
	SzId() const override
	{
		return "CXformJoin2BitmapIndexGetApply";
	}

};	// class CXformJoin2BitmapIndexGetApply

}  // namespace gpopt

#endif	// !GPOPT_CXformJoin2BitmapIndexGetApply_H

// EOF
