//---------------------------------------------------------------------------
//	@filename:
//		CXformExploration.cpp
//
//	@doc:
//		Implementation of basic exploration transformation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformExploration.h"
#include "duckdb/optimizer/cascade/base.h"

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