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
#include "duckdb/optimizer/cascade/common/CRefCount.h"

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
class CXformContext : public CRefCount
{
private:
	// Memory pool
	CMemoryPool *m_mp;

	// private copy ctor
	CXformContext(const CXformContext &);

public:
	// ctor
	explicit CXformContext(CMemoryPool *mp) : m_mp(mp)
	{
	}

	// dtor
	~CXformContext()
	{
	}


	// accessor
	inline CMemoryPool *
	Pmp() const
	{
		return m_mp;
	}

};	// class CXformContext

}  // namespace gpopt

#endif
