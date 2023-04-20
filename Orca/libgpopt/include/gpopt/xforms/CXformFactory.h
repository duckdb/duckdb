//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformFactory.h
//
//	@doc:
//		Management of global xform set
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformFactory_H
#define GPOPT_CXformFactory_H

#include "gpos/base.h"

#include "gpopt/xforms/CXform.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformFactory
//
//	@doc:
//		Factory class to manage xforms
//
//---------------------------------------------------------------------------
class CXformFactory
{
private:
	// definition of hash map to maintain mappings
	typedef CHashMap<CHAR, CXform, gpos::HashValue<CHAR>, CXform::FEqualIds,
					 CleanupDeleteArray<CHAR>, CleanupNULL<CXform> >
		XformNameToXformMap;

	// memory pool
	CMemoryPool *m_mp;

	// range of all xforms
	CXform *m_rgpxf[CXform::ExfSentinel];

	// name -> xform map
	XformNameToXformMap *m_phmszxform;

	// bitset of exploration xforms
	CXformSet *m_pxfsExploration;

	// bitset of implementation xforms
	CXformSet *m_pxfsImplementation;

	// global instance
	static CXformFactory *m_pxff;

	// private ctor
	explicit CXformFactory(CMemoryPool *mp);

	// private copy ctor
	CXformFactory(const CXformFactory &);

	// actual adding of xform
	void Add(CXform *pxform);


public:
	// dtor
	~CXformFactory();

	// create all xforms
	void Instantiate();

	// accessor by xform id
	CXform *Pxf(CXform::EXformId exfid) const;

	// accessor by xform name
	CXform *Pxf(const CHAR *szXformName) const;

	// accessor of exploration xforms
	CXformSet *
	PxfsExploration() const
	{
		return m_pxfsExploration;
	}

	// accessor of implementation xforms
	CXformSet *
	PxfsImplementation() const
	{
		return m_pxfsImplementation;
	}

	// global accessor
	static CXformFactory *
	Pxff()
	{
		return m_pxff;
	}

	// initialize global factory instance
	static GPOS_RESULT Init();

	// destroy global factory instance
	void Shutdown();

};	// class CXformFactory

}  // namespace gpopt


#endif	// !GPOPT_CXformFactory_H

// EOF
