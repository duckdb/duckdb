//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementTVFNoArgs.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementTVFNoArgs.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVFNoArgs::CXformImplementTVFNoArgs
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementTVFNoArgs::CXformImplementTVFNoArgs(CMemoryPool *mp)
	: CXformImplementTVF(
		  // pattern
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalTVF(mp)))
{
}

// EOF
