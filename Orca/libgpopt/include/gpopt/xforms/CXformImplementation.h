//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformImplementation.h
//
//	@doc:
//		Base class for implementation transforms
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementation_H
#define GPOPT_CXformImplementation_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXform.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementation
//
//	@doc:
//		base class for all implementations
//
//---------------------------------------------------------------------------
class CXformImplementation : public CXform
{
private:
	// private copy ctor
	CXformImplementation(const CXformImplementation &);

public:
	// ctor
	explicit CXformImplementation(CExpression *);

	// dtor
	virtual ~CXformImplementation();

	// type of operator
	virtual BOOL
	FImplementation() const
	{
		GPOS_ASSERT(!FSubstitution() && !FExploration());
		return true;
	}

};	// class CXformImplementation

}  // namespace gpopt


#endif	// !GPOPT_CXformImplementation_H

// EOF
