//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformGbAgg2Apply.h
//
//	@doc:
//		Transform GbAgg to Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGbAgg2Apply_H
#define GPOPT_CXformGbAgg2Apply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformSubqueryUnnest.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGbAgg2Apply
//
//	@doc:
//		Transform GbAgg to Apply; this transformation is only applicable
//		to GbAgg expression with aggregate functions that have subquery
//		arguments
//
//---------------------------------------------------------------------------
class CXformGbAgg2Apply : public CXformSubqueryUnnest
{
private:
	// private copy ctor
	CXformGbAgg2Apply(const CXformGbAgg2Apply &);

public:
	// ctor
	explicit CXformGbAgg2Apply(CMemoryPool *mp);

	// dtor
	virtual ~CXformGbAgg2Apply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfGbAgg2Apply;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformGbAgg2Apply";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

};	// class CXformGbAgg2Apply

}  // namespace gpopt

#endif	// !GPOPT_CXformGbAgg2Apply_H

// EOF
