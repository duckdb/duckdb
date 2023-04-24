//---------------------------------------------------------------------------
//	@filename:
//		CMDName.h
//
//	@doc:
//		Class for representing metadata names.
//---------------------------------------------------------------------------

#ifndef GPMD_CMDName_H
#define GPMD_CMDName_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMDName
//
//	@doc:
//		Class for representing metadata names.
//
//---------------------------------------------------------------------------
class CMDName
{
private:
	// the string holding the name
	const CWStringConst *m_name;

	// keep track of copy status
	BOOL m_deep_copy;

public:
	// ctor/dtor
	CMDName(CMemoryPool *mp, const CWStringBase *str);
	CMDName(const CWStringConst *, BOOL fOwnsMemory = false);

	// shallow copy ctor
	CMDName(const CMDName &);

	~CMDName();

	// accessors
	const CWStringConst *
	GetMDName() const
	{
		return m_name;
	}
};

// array of names
typedef CDynamicPtrArray<CMDName, CleanupDelete> CMDNameArray;
}  // namespace gpmd

#endif
