//---------------------------------------------------------------------------
//	@filename:
//		CXformImplementation.cpp
//
//	@doc:
//		Implementation of basic implementation transformation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformImplementation.h"
#include "duckdb/optimizer/cascade/base.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementation::CXformImplementation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CXformImplementation::CXformImplementation(duckdb::unique_ptr<Operator> pop)
    : CXform(std::move(pop))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementation::~CXformImplementation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CXformImplementation::~CXformImplementation()
{
}