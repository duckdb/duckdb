//---------------------------------------------------------------------------
//  Greenplum Database
//	Copyright (C) 2018-Present VMware, Inc. or its affiliates.
//
//	@filename:
//		CContextQueryToDXL.cpp
//
//	@doc:
//		Implementation of the methods used to hold information about
//		the whole query, when translate a query into DXL tree. All
//		translator methods allocate memory in the provided memory pool,
//		and the caller is responsible for freeing it
//
//---------------------------------------------------------------------------
extern "C" {
#include "postgres.h"
}

#include "gpopt/translate/CContextQueryToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CIdGenerator.h"

using namespace gpdxl;

CContextQueryToDXL::CContextQueryToDXL(CMemoryPool *mp)
	: m_mp(mp),
	  m_has_distributed_tables(false),
	  m_distribution_hashops(DistrHashOpsNotDeterminedYet)
{
	// map that stores gpdb att to optimizer col mapping
	m_colid_counter = GPOS_NEW(mp) CIdGenerator(GPDXL_COL_ID_START);
	m_cte_id_counter = GPOS_NEW(mp) CIdGenerator(GPDXL_CTE_ID_START);
}

CContextQueryToDXL::~CContextQueryToDXL()
{
	GPOS_DELETE(m_colid_counter);
	GPOS_DELETE(m_cte_id_counter);
}
