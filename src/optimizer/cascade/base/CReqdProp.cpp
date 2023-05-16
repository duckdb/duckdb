//---------------------------------------------------------------------------
//	@filename:
//		CReqdProp.cpp
//
//	@doc:
//		Implementation of required properties
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CReqdProp.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/COperator.h"

#ifdef GPOS_DEBUG
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#endif	// GPOS_DEBUG

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CReqdProp::CReqdProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdProp::CReqdProp()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdProp::~CReqdProp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdProp::~CReqdProp()
{
}