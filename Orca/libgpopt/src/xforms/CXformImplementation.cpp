//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformImplementation.cpp
//
//	@doc:
//		Implementation of basic implementation transformation
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementation.h"

#include "gpos/base.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementation::CXformImplementation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CXformImplementation::CXformImplementation(CExpression *pexpr) : CXform(pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementation::~CXformImplementation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CXformImplementation::~CXformImplementation()
{
}


// EOF
