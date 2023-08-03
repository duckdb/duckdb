//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxtPlan.cpp
//
//	@doc:
//		Derived plan properties context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::CDrvdPropCtxtPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropCtxtPlan::CDrvdPropCtxtPlan(BOOL fUpdateCTEMap)
	: CDrvdPropCtxt()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::~CDrvdPropCtxtPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropCtxtPlan::~CDrvdPropCtxtPlan()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::PdpctxtCopy
//
//	@doc:
//		Copy function
//
//---------------------------------------------------------------------------
CDrvdPropCtxt* CDrvdPropCtxtPlan::PdpctxtCopy() const
{
	CDrvdPropCtxtPlan* pdpctxtplan = new CDrvdPropCtxtPlan();
	return pdpctxtplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::AddProps
//
//	@doc:
//		Add props to context
//
//---------------------------------------------------------------------------
void CDrvdPropCtxtPlan::AddProps(CDrvdProp* pdp)
{
	return;
}