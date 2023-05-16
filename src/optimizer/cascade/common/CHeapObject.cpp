//---------------------------------------------------------------------------
//	@filename:
//		CHeapObject.cpp
//
//	@doc:
//		Implementation of class of all objects that must reside on the heap;
//		There used to be an assertion for that here, but it was too fragile.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CHeapObject.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CHeapObject::CHeapObject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CHeapObject::CHeapObject()
{
}