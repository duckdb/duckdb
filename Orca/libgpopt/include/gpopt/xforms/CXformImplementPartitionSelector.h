//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CXformImplementPartitionSelector.h
//
//	@doc:
//		Implement PartitionSelector
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementPartitionSelector_H
#define GPOPT_CXformImplementPartitionSelector_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementPartitionSelector
//
//	@doc:
//		Implement PartitionSelector
//
//---------------------------------------------------------------------------
class CXformImplementPartitionSelector : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementPartitionSelector(const CXformImplementPartitionSelector &);

public:
	// ctor
	explicit CXformImplementPartitionSelector(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementPartitionSelector()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementPartitionSelector;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementPartitionSelector";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &  //exprhdl
	) const
	{
		return CXform::ExfpHigh;
	}

	// actual transform
	virtual void Transform(CXformContext *, CXformResult *,
						   CExpression *) const;

};	// class CXformImplementPartitionSelector

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementPartitionSelector_H

// EOF
