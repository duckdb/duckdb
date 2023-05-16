//---------------------------------------------------------------------------
//	@filename:
//		CStackObject.cpp
//
//	@doc:
//		Implementation of classes of all objects that must reside on the stack;
//		There used to be an assertion for that here, but it was too fragile.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CStackObject.h"
#include "duckdb/optimizer/cascade/utils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CStackObject::CStackObject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStackObject::CStackObject()
{
}