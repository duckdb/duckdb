//---------------------------------------------------------------------------
//	@filename:
//		CXformFactory.h
//
//	@doc:
//		Management of global xform set
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformFactory
//
//	@doc:
//		Factory class to manage xforms
//
//---------------------------------------------------------------------------
class CXformFactory {
public:
	// range of all xforms
	CXform *m_xform_range[CXform::ExfSentinel];
	// name -> xform map
	unordered_map<CHAR *, CXform *> m_xform_dict;
	// bitset of exploration xforms
	CXform_set *m_exploration_xforms;
	// bitset of implementation xforms
	CXform_set *m_implementation_xforms;
	// global instance
	static CXformFactory *m_xform_factory;

public:
	// ctor
	explicit CXformFactory();
	// np copy ctor
	CXformFactory(const CXformFactory &) = delete;
	// dtor
	~CXformFactory();

public:
	// actual adding of xform
	void Add(CXform *xform);
	// create all xforms
	void Instantiate();
	// accessor by xform id
	CXform *Xform(CXform::EXformId xform_id) const;
	// accessor by xform name
	CXform *Xform(const CHAR *xform_name) const;
	// accessor of exploration xforms
	CXform_set *XformExploration() const {
		return m_exploration_xforms;
	}
	// accessor of implementation xforms
	CXform_set *XformImplementation() const {
		return m_implementation_xforms;
	}
	// global accessor
	static CXformFactory *XformFactory() {
		return m_xform_factory;
	}
	// initialize global factory instance
	static GPOS_RESULT Init();
	// destroy global factory instance
	void Shutdown();
}; // class CXformFactory
} // namespace gpopt