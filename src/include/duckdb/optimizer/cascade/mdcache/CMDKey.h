//---------------------------------------------------------------------------
//	@filename:
//		CMDKey.h
//
//	@doc:
//		Key for metadata objects in the cache
//---------------------------------------------------------------------------
#ifndef GPOPT_CMDKey_H
#define GPOPT_CMDKey_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"

#include "duckdb/optimizer/cascade/md/IMDId.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CMDKey
//
//	@doc:
//		Key for metadata objects in the cache
//
//---------------------------------------------------------------------------
class CMDKey
{
private:
	// id of the object in the underlying source
	const IMDId *m_mdid;

public:
	// ctors
	explicit CMDKey(const IMDId *mdid);

	// dtor
	~CMDKey()
	{
	}


	const IMDId *
	MDId() const
	{
		return m_mdid;
	}

	// equality function
	BOOL Equals(const CMDKey &mdkey) const;

	// hash function
	ULONG HashValue() const;


	// equality function for using MD keys in a cache
	static BOOL FEqualMDKey(CMDKey *const &pvLeft, CMDKey *const &pvRight);

	// hash function for using MD keys in a cache
	static ULONG UlHashMDKey(CMDKey *const &pv);
};
}  // namespace gpopt

#endif