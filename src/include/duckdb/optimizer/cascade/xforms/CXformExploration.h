//---------------------------------------------------------------------------
//	@filename:
//		CXformExploration.h
//
//	@doc:
//		Base class for exploration transforms
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExploration_H
#define GPOPT_CXformExploration_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"
#include "duckdb/common/unique_ptr.hpp"

using namespace duckdb;
using namespace gpos;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CXformExploration
//
//	@doc:
//		Base class for all explorations
//
//---------------------------------------------------------------------------
class CXformExploration : public CXform
{
public:
	// ctor
	explicit CXformExploration(duckdb::unique_ptr<Operator> pexpr);

	// private copy ctor
	CXformExploration(const CXformExploration &) = delete;

	// dtor
	virtual ~CXformExploration();

	// type of operator
	virtual bool FExploration() const
	{
		return true;
	}

	// is transformation a subquery unnesting (Subquery To Apply) xform?
	virtual bool FSubqueryUnnesting() const
	{
		return false;
	}

	// is transformation an Apply decorrelation (Apply To Join) xform?
	virtual bool FApplyDecorrelating() const
	{
		return false;
	}

	// do stats need to be computed before applying xform?
	virtual bool FNeedsStats() const
	{
		return false;
	}

	// conversion function
	static CXformExploration* Pxformexp(CXform* pxform)
	{
		return dynamic_cast<CXformExploration*>(pxform);
	}
};	// class CXformExploration
}  // namespace gpopt
#endif