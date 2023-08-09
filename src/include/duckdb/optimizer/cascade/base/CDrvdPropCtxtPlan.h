//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxtPlan.h
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of plan properties
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxtPlan_H
#define GPOPT_CDrvdPropCtxtPlan_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxt.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"

using namespace gpos;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropCtxtPlan
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of plan properties
//
//---------------------------------------------------------------------------
class CDrvdPropCtxtPlan : public CDrvdPropCtxt
{
public:
	// ctor
	CDrvdPropCtxtPlan(bool fUpdateCTEMap = true);

	// no copy ctor
	CDrvdPropCtxtPlan(const CDrvdPropCtxtPlan &) = delete;

	// dtor
	~CDrvdPropCtxtPlan() override;

public:
	// copy function
	CDrvdPropCtxt* PdpctxtCopy() const override;

	// add props to context
	void AddProps(CDrvdProp* pdp) override;

public:
	// conversion function
	static CDrvdPropCtxtPlan* PdpctxtplanConvert(CDrvdPropCtxt* pdpctxt)
	{
		return reinterpret_cast<CDrvdPropCtxtPlan*>(pdpctxt);
	}
};	// class CDrvdPropCtxtPlan
}  // namespace gpopt
#endif