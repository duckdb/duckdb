//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSequenceProject2Apply.h
//
//	@doc:
//		Transform Sequence Project to Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSequenceProject2Apply_H
#define GPOPT_CXformSequenceProject2Apply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformSubqueryUnnest.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSequenceProject2Apply
//
//	@doc:
//		Transform Sequence Project to Apply; this transformation is only
//		applicable to Sequence Project expression with window functions that
//		have subquery arguments
//
//---------------------------------------------------------------------------
class CXformSequenceProject2Apply : public CXformSubqueryUnnest
{
private:
	// private copy ctor
	CXformSequenceProject2Apply(const CXformSequenceProject2Apply &);

public:
	// ctor
	explicit CXformSequenceProject2Apply(CMemoryPool *mp);

	// dtor
	virtual ~CXformSequenceProject2Apply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSequenceProject2Apply;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSequenceProject2Apply";
	}

};	// class CXformSequenceProject2Apply

}  // namespace gpopt

#endif	// !GPOPT_CXformSequenceProject2Apply_H

// EOF
