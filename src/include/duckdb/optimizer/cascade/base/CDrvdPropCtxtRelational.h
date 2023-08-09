//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxtRelational.h
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of relational properties
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxtRelational_H
#define GPOPT_CDrvdPropCtxtRelational_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxt.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropCtxtRelational
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of relational properties
//
//---------------------------------------------------------------------------
class CDrvdPropCtxtRelational : public CDrvdPropCtxt
{
public:
	// copy function
	virtual CDrvdPropCtxt* PdpctxtCopy() const
	{
		return new CDrvdPropCtxtRelational();
	}

	// add props to context
	virtual void AddProps(CDrvdProp* pdp)
	{
		// derived relational context is currently empty
	}

public:
	// ctor
	CDrvdPropCtxtRelational()
		: CDrvdPropCtxt()
	{
	}
	
	// no copy ctor
	CDrvdPropCtxtRelational(const CDrvdPropCtxtRelational &) = delete;

	// dtor
	virtual ~CDrvdPropCtxtRelational()
	{
	}

	// conversion function
	static CDrvdPropCtxtRelational* PdpctxtrelConvert(CDrvdPropCtxt* pdpctxt)
	{
		return static_cast<CDrvdPropCtxtRelational*>(pdpctxt);
	}
};	// class CDrvdPropCtxtRelational
}  // namespace gpopt
#endif