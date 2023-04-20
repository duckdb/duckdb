//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSelect2Apply.h
//
//	@doc:
//		Transform Select to Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSelect2Apply_H
#define GPOPT_CXformSelect2Apply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformSubqueryUnnest.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSelect2Apply
//
//	@doc:
//		Transform Select to Apply; this transformation is only applicable
//		to a Select expression with subqueries in its scalar predicate
//
//---------------------------------------------------------------------------
class CXformSelect2Apply : public CXformSubqueryUnnest
{
private:
	// private copy ctor
	CXformSelect2Apply(const CXformSelect2Apply &);

public:
	// ctor
	explicit CXformSelect2Apply(CMemoryPool *mp);

	// dtor
	virtual ~CXformSelect2Apply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSelect2Apply;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSelect2Apply";
	}

};	// class CXformSelect2Apply

}  // namespace gpopt

#endif	// !GPOPT_CXformSelect2Apply_H

// EOF
