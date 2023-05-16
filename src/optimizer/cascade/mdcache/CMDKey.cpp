//---------------------------------------------------------------------------
//	@filename:
//		CMDKey.cpp
//
//	@doc:
//		Implementation of a key for metadata cache objects
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/mdcache/CMDKey.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"

using namespace gpos;
using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::CMDKey
//
//	@doc:
//		Constructs a md cache key
//
//---------------------------------------------------------------------------
CMDKey::CMDKey(const IMDId *mdid) : m_mdid(mdid)
{
	GPOS_ASSERT(mdid->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CMDKey::Equals(const CMDKey &mdkey) const
{
	return mdkey.MDId()->Equals(m_mdid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::FEqualMDKey
//
//	@doc:
//		Equality function for using MD keys in a cache
//
//---------------------------------------------------------------------------
BOOL
CMDKey::FEqualMDKey(CMDKey *const &pvLeft, CMDKey *const &pvRight)
{
	if (NULL == pvLeft && NULL == pvRight)
	{
		return true;
	}

	if (NULL == pvLeft || NULL == pvRight)
	{
		return false;
	}

	GPOS_ASSERT(NULL != pvLeft && NULL != pvRight);

	return pvLeft->MDId()->Equals(pvRight->MDId());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CMDKey::HashValue() const
{
	return m_mdid->HashValue();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDKey::UlHashMDKey
//
//	@doc:
//		Hash function for using MD keys in a cache
//
//---------------------------------------------------------------------------
ULONG CMDKey::UlHashMDKey(CMDKey *const &pv)
{
	return pv->MDId()->HashValue();
}