//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxt.h
//
//	@doc:
//		Base class for derived properties context;
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxt_H
#define GPOPT_CDrvdPropCtxt_H

#include "duckdb/optimizer/cascade/base.h"
#include <memory>

using namespace gpos;
using namespace std;

namespace gpopt
{
// fwd declarations
class CDrvdPropCtxt;
class CDrvdProp;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropCtxt
//
//	@doc:
//		Container of information passed among expression nodes during
//		property derivation
//
//---------------------------------------------------------------------------
class CDrvdPropCtxt
{
public:
	// ctor
	CDrvdPropCtxt()
	{
	}

	// no copy ctor
	CDrvdPropCtxt(const CDrvdPropCtxt &) = delete;

	// dtor
	virtual ~CDrvdPropCtxt()
	{
	}

public:
	// copy function
	virtual CDrvdPropCtxt* PdpctxtCopy() const = 0;

	// add props to context
	virtual void AddProps(CDrvdProp* pdp) = 0;

public:
	// copy function
	static CDrvdPropCtxt* PdpctxtCopy(CDrvdPropCtxt* pdpctxt)
	{
		if (nullptr == pdpctxt)
		{
			return nullptr;
		}
		return pdpctxt->PdpctxtCopy();
	}

	// add derived props to context
	static void AddDerivedProps(CDrvdProp* pdp, CDrvdPropCtxt* pdpctxt)
	{
		if (nullptr != pdpctxt)
		{
			pdpctxt->AddProps(pdp);
		}
	}
};	// class CDrvdPropCtxt
}  // namespace gpopt
#endif