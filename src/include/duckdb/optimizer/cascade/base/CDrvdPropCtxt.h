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
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

namespace gpopt
{
using namespace gpos;

// fwd declarations
class CDrvdPropCtxt;
class CDrvdProp;

// dynamic array for properties
typedef CDynamicPtrArray<CDrvdPropCtxt, CleanupRelease> CDrvdPropCtxtArray;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropCtxt
//
//	@doc:
//		Container of information passed among expression nodes during
//		property derivation
//
//---------------------------------------------------------------------------
class CDrvdPropCtxt : public CRefCount
{
private:
	// private copy ctor
	CDrvdPropCtxt(const CDrvdPropCtxt &);

protected:
	// memory pool
	CMemoryPool *m_mp;

	// copy function
	virtual CDrvdPropCtxt *PdpctxtCopy(CMemoryPool *mp) const = 0;

	// add props to context
	virtual void AddProps(CDrvdProp *pdp) = 0;

public:
	// ctor
	CDrvdPropCtxt(CMemoryPool *mp) : m_mp(mp)
	{
	}

	// dtor
	virtual ~CDrvdPropCtxt()
	{
	}

#ifdef GPOS_DEBUG

	// is it a relational property context?
	virtual BOOL
	FRelational() const
	{
		return false;
	}

	// is it a plan property context?
	virtual BOOL
	FPlan() const
	{
		return false;
	}

	// is it a scalar property context?
	virtual BOOL
	FScalar() const
	{
		return false;
	}

#endif	// GPOS_DEBUG

	// copy function
	static CDrvdPropCtxt *
	PdpctxtCopy(CMemoryPool *mp, CDrvdPropCtxt *pdpctxt)
	{
		if (NULL == pdpctxt)
		{
			return NULL;
		}

		return pdpctxt->PdpctxtCopy(mp);
	}

	// add derived props to context
	static void
	AddDerivedProps(CDrvdProp *pdp, CDrvdPropCtxt *pdpctxt)
	{
		if (NULL != pdpctxt)
		{
			pdpctxt->AddProps(pdp);
		}
	}

};	// class CDrvdPropCtxt

// shorthand for printing
IOstream &operator<<(IOstream &os, CDrvdPropCtxt &drvdpropctxt);

}  // namespace gpopt

#endif