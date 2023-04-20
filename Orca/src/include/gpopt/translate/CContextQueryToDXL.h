//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CContextQueryToDXL.h
//
//	@doc:
//		Class to hold information about a whole top-level query, while
//		recursively translating a Query tree to DXL tree.
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CContextQueryToDXL_H
#define GPDXL_CContextQueryToDXL_H

#include "gpos/base.h"

#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#define GPDXL_CTE_ID_START 1
#define GPDXL_COL_ID_START 1

namespace gpdxl
{
// fwd declarations
class CTranslatorQueryToDXL;
class CTranslatorScalarToDXL;

//---------------------------------------------------------------------------
//	@class:
//		CContextQueryToDXL
//
//	@doc:
//		Class to hold information about a whole top-level query, while
//		recursively translating a Query tree to DXL tree.
//
//---------------------------------------------------------------------------
class CContextQueryToDXL
{
	friend class CTranslatorQueryToDXL;
	friend class CTranslatorScalarToDXL;

private:
	// memory pool
	CMemoryPool *m_mp;

	// counter for generating unique column ids
	CIdGenerator *m_colid_counter;

	// counter for generating unique CTE ids
	CIdGenerator *m_cte_id_counter;

	// does the query have any distributed tables?
	BOOL m_has_distributed_tables;

	// What operator classes are used in the distribution keys?
	DistributionHashOpsKind m_distribution_hashops;

public:
	// ctor
	CContextQueryToDXL(CMemoryPool *mp);

	// dtor
	~CContextQueryToDXL();
};
}  // namespace gpdxl
#endif	// GPDXL_CContextQueryToDXL_H

//EOF
