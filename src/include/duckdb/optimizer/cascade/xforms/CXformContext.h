//---------------------------------------------------------------------------
//	@filename:
//		CXformContext.h
//
//	@doc:
//		Context container passed to every application of a transformation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformContext_H
#define GPOPT_CXformContext_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPatternTree.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformContext
//
//	@doc:
//		context container
//
//---------------------------------------------------------------------------
class CXformContext
{
public:
	// ctor
	explicit CXformContext()
	{
	}

	// no copy ctor
	CXformContext(const CXformContext &) = delete;

	// dtor
	~CXformContext()
	{
	}
};	// class CXformContext
}  // namespace gpopt
#endif