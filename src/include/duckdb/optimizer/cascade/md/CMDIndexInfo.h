//---------------------------------------------------------------------------
//	@filename:
//		CMDIndexInfo.h
//
//	@doc:
//		Implementation of indexinfo in relation metadata
//---------------------------------------------------------------------------
#ifndef GPMD_CMDIndexInfo_H
#define GPMD_CMDIndexInfo_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/md/IMDId.h"
#include "duckdb/optimizer/cascade/md/IMDInterface.h"

namespace gpmd
{
using namespace gpos;

// class for indexinfo in relation metadata
class CMDIndexInfo : public IMDInterface
{
private:
	// index mdid
	IMDId *m_mdid;

	// is the index partial
	BOOL m_is_partial;

public:
	// ctor
	CMDIndexInfo(IMDId *mdid, BOOL is_partial);

	// dtor
	virtual ~CMDIndexInfo();

	// index mdid
	IMDId *MDId() const;

	// is the index partial
	BOOL IsPartial() const;

#ifdef GPOS_DEBUG
	// debug print of the index info
	virtual void DebugPrint(IOstream &os) const;
#endif
};

}  // namespace gpmd

#endif
