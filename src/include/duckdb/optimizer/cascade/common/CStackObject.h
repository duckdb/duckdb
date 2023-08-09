//---------------------------------------------------------------------------
//	@filename:
//		CStackObject.h
//
//	@doc:
//		Class of all objects that must reside on the stack;
//		e.g., auto objects;
//---------------------------------------------------------------------------
#ifndef GPOS_CStackObject_H
#define GPOS_CStackObject_H

#include "duckdb/optimizer/cascade/types.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CStackObject
//
//	@doc:
//		Constructor tests stack layout to ensure object is allocated on stack;
//		constructor is protected to prevent direct instantiation of class;
//
//---------------------------------------------------------------------------
class CStackObject
{
public:
	CStackObject();
};
}  // namespace gpos
#endif