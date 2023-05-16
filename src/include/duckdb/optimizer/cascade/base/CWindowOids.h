//---------------------------------------------------------------------------
//	@filename:
//		CWindowOids.h
//
//	@doc:
//		System specific oids for window operations
//---------------------------------------------------------------------------
#ifndef GPOPT_CWindowOids_H
#define GPOPT_CWindowOids_H

#include "duckdb/optimizer/cascade/common/CRefCount.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"

#define DUMMY_ROW_NUMBER_OID OID(7000)
#define DUMMY_WIN_RANK OID(7001)


namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CWindowOids
//
//	@doc:
//		GPDB specific oids
//
//---------------------------------------------------------------------------
class CWindowOids : public CRefCount
{
private:
	// oid of window operation "row_number" function
	OID m_oidRowNumber;

	// oid of window operation "rank" function
	OID m_oidRank;

public:
	CWindowOids(OID row_number_oid, OID rank_oid);

	// accessor of oid value of "row_number" function
	OID OidRowNumber() const;

	// accessor of oid value of "rank" function
	OID OidRank() const;

	// generate default window oids
	static CWindowOids *GetWindowOids(CMemoryPool *mp);

};	// class CWindowOids
}  // namespace gpopt

#endif