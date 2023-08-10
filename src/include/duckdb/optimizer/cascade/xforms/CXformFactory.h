//---------------------------------------------------------------------------
//	@filename:
//		CXformFactory.h
//
//	@doc:
//		Management of global xform set
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformFactory_H
#define GPOPT_CXformFactory_H

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
	CXform *m_rgpxf[CXform::ExfSentinel];

	// name -> xform map
	unordered_map<CHAR *, CXform *> m_phmszxform;

	// bitset of exploration xforms
	CXformSet *m_pxfsExploration;

	// bitset of implementation xforms
	CXformSet *m_pxfsImplementation;

	// global instance
	static CXformFactory *m_pxff;

public:
	// ctor
	explicit CXformFactory();

	// np copy ctor
	CXformFactory(const CXformFactory &) = delete;

	// dtor
	~CXformFactory();

public:
	// actual adding of xform
	void Add(CXform *pxform);

	// create all xforms
	void Instantiate();

	// accessor by xform id
	CXform *Pxf(CXform::EXformId exfid) const;

	// accessor by xform name
	CXform *Pxf(const CHAR *szXformName) const;

	// accessor of exploration xforms
	CXformSet *PxfsExploration() const {
		return m_pxfsExploration;
	}

	// accessor of implementation xforms
	CXformSet *PxfsImplementation() const {
		return m_pxfsImplementation;
	}

	// global accessor
	static CXformFactory *Pxff() {
		return m_pxff;
	}

	// initialize global factory instance
	static GPOS_RESULT Init();

	// destroy global factory instance
	void Shutdown();
}; // class CXformFactory
} // namespace gpopt
#endif