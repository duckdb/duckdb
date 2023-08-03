//---------------------------------------------------------------------------
//	@filename:
//		CXformImplementation.h
//
//	@doc:
//		Base class for implementation transforms
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementation_H
#define GPOPT_CXformImplementation_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

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
	explicit CXformImplementation(duckdb::unique_ptr<Operator> op);

	// dtor
	virtual ~CXformImplementation();

	// type of operator
	virtual BOOL FImplementation() const
	{
		return true;
	}

};	// class CXformImplementation
}  // namespace gpopt

#endif