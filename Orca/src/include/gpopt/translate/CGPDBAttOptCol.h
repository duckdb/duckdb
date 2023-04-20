//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Greenplum
//
//	@filename:
//		CGPDBAttOptCol.h
//
//	@doc:
//		Class to represent pair of GPDB var info to optimizer col info
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CGPDBAttOptCol_H
#define GPDXL_CGPDBAttOptCol_H

#include "gpos/common/CRefCount.h"

#include "gpopt/translate/CGPDBAttInfo.h"
#include "gpopt/translate/COptColInfo.h"

namespace gpdxl
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CGPDBAttOptCol
//
//	@doc:
//		Class to represent pair of GPDB var info to optimizer col info
//
//---------------------------------------------------------------------------
class CGPDBAttOptCol : public CRefCount
{
private:
	// gpdb att info
	CGPDBAttInfo *m_gpdb_att_info;

	// optimizer col info
	COptColInfo *m_opt_col_info;

public:
	CGPDBAttOptCol(const CGPDBAttOptCol &) = delete;

	// ctor
	CGPDBAttOptCol(CGPDBAttInfo *gpdb_att_info, COptColInfo *opt_col_info)
		: m_gpdb_att_info(gpdb_att_info), m_opt_col_info(opt_col_info)
	{
		GPOS_ASSERT(NULL != m_gpdb_att_info);
		GPOS_ASSERT(NULL != m_opt_col_info);
	}

	// d'tor
	~CGPDBAttOptCol() override
	{
		m_gpdb_att_info->Release();
		m_opt_col_info->Release();
	}

	// accessor
	const CGPDBAttInfo *
	GetGPDBAttInfo() const
	{
		return m_gpdb_att_info;
	}

	// accessor
	const COptColInfo *
	GetOptColInfo() const
	{
		return m_opt_col_info;
	}
};

}  // namespace gpdxl

#endif	// !GPDXL_CGPDBAttOptCol_H

// EOF
