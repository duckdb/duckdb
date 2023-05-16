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
private:
	// private copy ctor
	CDrvdPropCtxtRelational(const CDrvdPropCtxtRelational &);

protected:
	// copy function
	virtual CDrvdPropCtxt *
	PdpctxtCopy(CMemoryPool *mp) const
	{
		return GPOS_NEW(mp) CDrvdPropCtxtRelational(mp);
	}

	// add props to context
	virtual void
	AddProps(CDrvdProp *  // pdp
	)
	{
		// derived relational context is currently empty
	}

public:
	// ctor
	CDrvdPropCtxtRelational(CMemoryPool *mp) : CDrvdPropCtxt(mp)
	{
	}

	// dtor
	virtual ~CDrvdPropCtxtRelational()
	{
	}

	// print
	virtual IOstream &
	OsPrint(IOstream &os) const
	{
		return os;
	}

#ifdef GPOS_DEBUG

	// is it a relational property context?
	virtual BOOL
	FRelational() const
	{
		return true;
	}

#endif	// GPOS_DEBUG

	// conversion function
	static CDrvdPropCtxtRelational *
	PdpctxtrelConvert(CDrvdPropCtxt *pdpctxt)
	{
		GPOS_ASSERT(NULL != pdpctxt);

		return reinterpret_cast<CDrvdPropCtxtRelational *>(pdpctxt);
	}

};	// class CDrvdPropCtxtRelational

}  // namespace gpopt

#endif