//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformExploration.cpp
//
//	@doc:
//		Implementation of basic exploration transformation
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformExploration.h"

#include "gpos/base.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExploration::CXformExploration
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExploration::CXformExploration(CExpression *pexpr) : CXform(pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExploration::~CXformExploration
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExploration::~CXformExploration()
{
}


// EOF
