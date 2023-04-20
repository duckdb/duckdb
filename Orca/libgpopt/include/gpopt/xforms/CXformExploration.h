//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformExploration.h
//
//	@doc:
//		Base class for exploration transforms
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExploration_H
#define GPOPT_CXformExploration_H

#include "gpos/base.h"

#include "gpopt/xforms/CXform.h"

namespace gpopt
{
using namespace gpos;

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
private:
	// private copy ctor
	CXformExploration(const CXformExploration &);

public:
	// ctor
	explicit CXformExploration(CExpression *pexpr);

	// dtor
	virtual ~CXformExploration();

	// type of operator
	virtual BOOL
	FExploration() const
	{
		GPOS_ASSERT(!FSubstitution() && !FImplementation());
		return true;
	}

	// is transformation a subquery unnesting (Subquery To Apply) xform?
	virtual BOOL
	FSubqueryUnnesting() const
	{
		return false;
	}

	// is transformation an Apply decorrelation (Apply To Join) xform?
	virtual BOOL
	FApplyDecorrelating() const
	{
		return false;
	}

	// do stats need to be computed before applying xform?
	virtual BOOL
	FNeedsStats() const
	{
		return false;
	}

	// conversion function
	static CXformExploration *
	Pxformexp(CXform *pxform)
	{
		GPOS_ASSERT(NULL != pxform);
		GPOS_ASSERT(pxform->FExploration());

		return dynamic_cast<CXformExploration *>(pxform);
	}

};	// class CXformExploration

}  // namespace gpopt


#endif	// !GPOPT_CXformExploration_H

// EOF
