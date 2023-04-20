//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementSequenceProject.h
//
//	@doc:
//		Transform Logical Sequence Project to Physical Sequence Project
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementSequenceProject_H
#define GPOPT_CXformImplementSequenceProject_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementSequenceProject
//
//	@doc:
//		Transform Project to ComputeScalar
//
//---------------------------------------------------------------------------
class CXformImplementSequenceProject : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementSequenceProject(const CXformImplementSequenceProject &);

public:
	// ctor
	explicit CXformImplementSequenceProject(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementSequenceProject()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementSequenceProject;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementSequenceProject";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &exprhdl) const
	{
		if (exprhdl.DeriveHasSubquery(1))
		{
			return CXform::ExfpNone;
		}

		return CXform::ExfpHigh;
	}

	// actual transform
	virtual void Transform(CXformContext *, CXformResult *,
						   CExpression *) const;

};	// class CXformImplementSequenceProject

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementSequenceProject_H

// EOF
