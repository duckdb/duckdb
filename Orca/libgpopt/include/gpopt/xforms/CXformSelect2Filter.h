//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSelect2Filter.h
//
//	@doc:
//		Transform Select to Filter
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSelect2Filter_H
#define GPOPT_CXformSelect2Filter_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSelect2Filter
//
//	@doc:
//		Transform Select to Filter
//
//---------------------------------------------------------------------------
class CXformSelect2Filter : public CXformImplementation
{
private:
	// private copy ctor
	CXformSelect2Filter(const CXformSelect2Filter &);

public:
	// ctor
	explicit CXformSelect2Filter(CMemoryPool *mp);

	// dtor
	virtual ~CXformSelect2Filter()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSelect2Filter;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformSelect2Filter";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformSelect2Filter

}  // namespace gpopt

#endif	// !GPOPT_CXformSelect2Filter_H

// EOF
