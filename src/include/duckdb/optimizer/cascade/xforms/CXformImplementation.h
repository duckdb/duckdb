//---------------------------------------------------------------------------
//	@filename:
//		CXformImplementation.h
//
//	@doc:
//		Base class for implementation transforms
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementation
//
//	@doc:
//		base class for all implementations
//
//---------------------------------------------------------------------------
class CXformImplementation : public CXform {
private:
	// private copy ctor
	CXformImplementation(const CXformImplementation &);

public:
	// ctor
	CXformImplementation() = default;
	explicit CXformImplementation(duckdb::unique_ptr<Operator> op) : CXform(std::move(op)) {};
	// dtor
	~CXformImplementation() override = default;
	// type of operator
	BOOL FImplementation() const override {
		return true;
	}
}; // class CXformImplementation
} // namespace gpopt