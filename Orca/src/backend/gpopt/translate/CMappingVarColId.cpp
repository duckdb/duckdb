//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMappingVarColId.cpp
//
//	@doc:
//		Implementation of base (abstract) var mapping class
//
//	@test:
//
//
//---------------------------------------------------------------------------
extern "C" {
#include "postgres.h"

#include "nodes/primnodes.h"
#include "nodes/value.h"
}
#include "gpos/error/CAutoTrace.h"

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CMappingVarColId.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/md/IMDIndex.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::CMappingVarColId
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMappingVarColId::CMappingVarColId(CMemoryPool *mp) : m_mp(mp)
{
	// This map can have many entries if there are many tables with many columns
	// in the query, so use a larger hash map to minimize collisions
	m_gpdb_att_opt_col_mapping =
		GPOS_NEW(m_mp) GPDBAttOptColHashMap(m_mp, 2047);
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::GetGPDBAttOptColMapping
//
//	@doc:
//		Given a gpdb attribute, return the mapping info to opt col
//
//---------------------------------------------------------------------------
const CGPDBAttOptCol *
CMappingVarColId::GetGPDBAttOptColMapping(
	ULONG current_query_level, const Var *var,
	EPlStmtPhysicalOpType plstmt_physical_op_type) const
{
	GPOS_ASSERT(NULL != var);
	GPOS_ASSERT(current_query_level >= var->varlevelsup);

	// absolute query level of var
	ULONG abs_query_level = current_query_level - var->varlevelsup;

	// extract varno
	ULONG var_no = var->varno;
	if (EpspotWindow == plstmt_physical_op_type ||
		EpspotAgg == plstmt_physical_op_type ||
		EpspotMaterialize == plstmt_physical_op_type)
	{
		// Agg and Materialize need to employ OUTER, since they have other
		// values in GPDB world
		var_no = OUTER_VAR;
	}

	CGPDBAttInfo *gpdb_att_info =
		GPOS_NEW(m_mp) CGPDBAttInfo(abs_query_level, var_no, var->varattno);
	CGPDBAttOptCol *gpdb_att_opt_col_info =
		m_gpdb_att_opt_col_mapping->Find(gpdb_att_info);

	if (NULL == gpdb_att_opt_col_info)
	{
		// TODO: Sept 09 2013, remove temporary fix (revert exception to assert) to avoid crash during algebrization
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLError,
				   GPOS_WSZ_LIT("No variable"));
	}

	gpdb_att_info->Release();
	return gpdb_att_opt_col_info;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::GetOptColName
//
//	@doc:
//		Given a gpdb attribute, return a column name in optimizer world
//
//---------------------------------------------------------------------------
const CWStringBase *
CMappingVarColId::GetOptColName(
	ULONG current_query_level, const Var *var,
	EPlStmtPhysicalOpType plstmt_physical_op_type) const
{
	return GetGPDBAttOptColMapping(current_query_level, var,
								   plstmt_physical_op_type)
		->GetOptColInfo()
		->GetOptColName();
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::GetColId
//
//	@doc:
//		given a gpdb attribute, return a column id in optimizer world
//
//---------------------------------------------------------------------------
ULONG
CMappingVarColId::GetColId(ULONG current_query_level, const Var *var,
						   EPlStmtPhysicalOpType plstmt_physical_op_type) const
{
	return GetGPDBAttOptColMapping(current_query_level, var,
								   plstmt_physical_op_type)
		->GetOptColInfo()
		->GetColId();
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::Insert
//
//	@doc:
//		Insert a single entry into the hash map
//
//---------------------------------------------------------------------------
void
CMappingVarColId::Insert(ULONG query_level, ULONG var_no, INT attrnum,
						 ULONG colid, CWStringBase *column_name)
{
	// GPDB agg node uses 0 in Var, but that should've been taken care of
	// by translator
	GPOS_ASSERT(var_no > 0);

	// create key
	CGPDBAttInfo *gpdb_att_info =
		GPOS_NEW(m_mp) CGPDBAttInfo(query_level, var_no, attrnum);

	// create value
	COptColInfo *opt_col_info = GPOS_NEW(m_mp) COptColInfo(colid, column_name);

	// key is part of value, bump up refcount
	gpdb_att_info->AddRef();
	CGPDBAttOptCol *gpdb_att_opt_col_info =
		GPOS_NEW(m_mp) CGPDBAttOptCol(gpdb_att_info, opt_col_info);

	BOOL result GPOS_ASSERTS_ONLY = m_gpdb_att_opt_col_mapping->Insert(
		gpdb_att_info, gpdb_att_opt_col_info);

	GPOS_ASSERT(result);
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::LoadTblColumns
//
//	@doc:
//		Load up information from GPDB's base table RTE and corresponding
//		optimizer table descriptor
//
//---------------------------------------------------------------------------
void
CMappingVarColId::LoadTblColumns(ULONG query_level, ULONG RTE_index,
								 const CDXLTableDescr *table_descr)
{
	GPOS_ASSERT(NULL != table_descr);
	const ULONG size = table_descr->Arity();

	// add mapping information for columns
	for (ULONG i = 0; i < size; i++)
	{
		const CDXLColDescr *dxl_col_descr = table_descr->GetColumnDescrAt(i);
		this->Insert(query_level, RTE_index, dxl_col_descr->AttrNum(),
					 dxl_col_descr->Id(),
					 dxl_col_descr->MdName()->GetMDName()->Copy(m_mp));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::LoadIndexColumns
//
//	@doc:
//		Load up information from GPDB index and corresponding
//		optimizer table descriptor
//
//---------------------------------------------------------------------------
void
CMappingVarColId::LoadIndexColumns(ULONG query_level, ULONG RTE_index,
								   const IMDIndex *index,
								   const CDXLTableDescr *table_descr)
{
	GPOS_ASSERT(NULL != table_descr);

	const ULONG size = index->Keys();

	// add mapping information for columns
	for (ULONG i = 0; i < size; i++)
	{
		ULONG pos = index->KeyAt(i);
		const CDXLColDescr *dxl_col_descr = table_descr->GetColumnDescrAt(pos);
		this->Insert(query_level, RTE_index, INT(i + 1), dxl_col_descr->Id(),
					 dxl_col_descr->MdName()->GetMDName()->Copy(m_mp));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::Load
//
//	@doc:
//		Load column mapping information from list of column names
//
//---------------------------------------------------------------------------
void
CMappingVarColId::Load(ULONG query_level, ULONG RTE_index,
					   CIdGenerator *id_generator, List *col_names)
{
	ListCell *col_name = NULL;
	ULONG i = 0;

	// add mapping information for columns
	ForEach(col_name, col_names)
	{
		Value *value = (Value *) lfirst(col_name);
		CHAR *col_name_char_array = strVal(value);

		CWStringDynamic *column_name =
			CDXLUtils::CreateDynamicStringFromCharArray(m_mp,
														col_name_char_array);

		this->Insert(query_level, RTE_index, INT(i + 1),
					 id_generator->next_id(), column_name->Copy(m_mp));

		i++;
		GPOS_DELETE(column_name);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::LoadColumns
//
//	@doc:
//		Load up columns information from the array of column descriptors
//
//---------------------------------------------------------------------------
void
CMappingVarColId::LoadColumns(ULONG query_level, ULONG RTE_index,
							  const CDXLColDescrArray *column_descrs)
{
	GPOS_ASSERT(NULL != column_descrs);
	const ULONG size = column_descrs->Size();

	// add mapping information for columns
	for (ULONG i = 0; i < size; i++)
	{
		const CDXLColDescr *dxl_col_descr = (*column_descrs)[i];
		this->Insert(query_level, RTE_index, dxl_col_descr->AttrNum(),
					 dxl_col_descr->Id(),
					 dxl_col_descr->MdName()->GetMDName()->Copy(m_mp));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::LoadDerivedTblColumns
//
//	@doc:
//		Load up information from column information in derived tables
//
//---------------------------------------------------------------------------
void
CMappingVarColId::LoadDerivedTblColumns(
	ULONG query_level, ULONG RTE_index,
	const CDXLNodeArray *derived_columns_dxl, List *target_list)
{
	GPOS_ASSERT(NULL != derived_columns_dxl);
	GPOS_ASSERT((ULONG) gpdb::ListLength(target_list) >=
				derived_columns_dxl->Size());

	ULONG drvd_tbl_col_counter =
		0;	// counter for the dynamic array of DXL nodes
	ListCell *lc = NULL;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		if (!target_entry->resjunk)
		{
			GPOS_ASSERT(0 < target_entry->resno);
			CDXLNode *dxlnode = (*derived_columns_dxl)[drvd_tbl_col_counter];
			GPOS_ASSERT(NULL != dxlnode);
			CDXLScalarIdent *dxl_sc_ident =
				CDXLScalarIdent::Cast(dxlnode->GetOperator());
			const CDXLColRef *dxl_colref = dxl_sc_ident->GetDXLColRef();
			this->Insert(query_level, RTE_index, INT(target_entry->resno),
						 dxl_colref->Id(),
						 dxl_colref->MdName()->GetMDName()->Copy(m_mp));
			drvd_tbl_col_counter++;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::LoadCTEColumns
//
//	@doc:
//		Load CTE column mappings
//
//---------------------------------------------------------------------------
void
CMappingVarColId::LoadCTEColumns(ULONG query_level, ULONG RTE_index,
								 const ULongPtrArray *CTE_columns,
								 List *target_list)
{
	GPOS_ASSERT(NULL != CTE_columns);
	GPOS_ASSERT((ULONG) gpdb::ListLength(target_list) >= CTE_columns->Size());

	ULONG idx = 0;
	ListCell *lc = NULL;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		if (!target_entry->resjunk)
		{
			GPOS_ASSERT(0 < target_entry->resno);
			ULONG CTE_colid = *((*CTE_columns)[idx]);

			CWStringDynamic *column_name =
				CDXLUtils::CreateDynamicStringFromCharArray(
					m_mp, target_entry->resname);
			this->Insert(query_level, RTE_index, INT(target_entry->resno),
						 CTE_colid, column_name);
			idx++;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::LoadProjectElements
//
//	@doc:
//		Load up information from projection list created from GPDB join expression
//
//---------------------------------------------------------------------------
void
CMappingVarColId::LoadProjectElements(ULONG query_level, ULONG RTE_index,
									  const CDXLNode *project_list_dxlnode)
{
	GPOS_ASSERT(NULL != project_list_dxlnode);
	const ULONG size = project_list_dxlnode->Arity();
	// add mapping information for columns
	for (ULONG i = 0; i < size; i++)
	{
		CDXLNode *dxlnode = (*project_list_dxlnode)[i];
		CDXLScalarProjElem *dxl_proj_elem =
			CDXLScalarProjElem::Cast(dxlnode->GetOperator());
		this->Insert(query_level, RTE_index, INT(i + 1), dxl_proj_elem->Id(),
					 dxl_proj_elem->GetMdNameAlias()->GetMDName()->Copy(m_mp));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::CopyMapColId
//
//	@doc:
//		Create a deep copy
//
//---------------------------------------------------------------------------
CMappingVarColId *
CMappingVarColId::CopyMapColId(ULONG query_level) const
{
	CMappingVarColId *var_colid_mapping = GPOS_NEW(m_mp) CMappingVarColId(m_mp);

	// iterate over full map
	GPDBAttOptColHashMapIter col_map_iterator(this->m_gpdb_att_opt_col_mapping);
	while (col_map_iterator.Advance())
	{
		const CGPDBAttOptCol *gpdb_att_opt_col_info = col_map_iterator.Value();
		const CGPDBAttInfo *gpdb_att_info =
			gpdb_att_opt_col_info->GetGPDBAttInfo();
		const COptColInfo *opt_col_info =
			gpdb_att_opt_col_info->GetOptColInfo();

		if (gpdb_att_info->GetQueryLevel() <= query_level)
		{
			// include all variables defined at same query level or before
			CGPDBAttInfo *gpdb_att_info_new = GPOS_NEW(m_mp) CGPDBAttInfo(
				gpdb_att_info->GetQueryLevel(), gpdb_att_info->GetVarNo(),
				gpdb_att_info->GetAttNo());
			COptColInfo *opt_col_info_new = GPOS_NEW(m_mp) COptColInfo(
				opt_col_info->GetColId(),
				GPOS_NEW(m_mp) CWStringConst(
					m_mp, opt_col_info->GetOptColName()->GetBuffer()));
			gpdb_att_info_new->AddRef();
			CGPDBAttOptCol *gpdb_att_opt_col_new = GPOS_NEW(m_mp)
				CGPDBAttOptCol(gpdb_att_info_new, opt_col_info_new);

			// insert into hashmap
			BOOL result GPOS_ASSERTS_ONLY =
				var_colid_mapping->m_gpdb_att_opt_col_mapping->Insert(
					gpdb_att_info_new, gpdb_att_opt_col_new);
			GPOS_ASSERT(result);
		}
	}

	return var_colid_mapping;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::CopyMapColId
//
//	@doc:
//		Create a deep copy
//
//---------------------------------------------------------------------------
CMappingVarColId *
CMappingVarColId::CopyMapColId(CMemoryPool *mp) const
{
	CMappingVarColId *var_colid_mapping = GPOS_NEW(mp) CMappingVarColId(mp);

	// iterate over full map
	GPDBAttOptColHashMapIter col_map_iterator(this->m_gpdb_att_opt_col_mapping);
	while (col_map_iterator.Advance())
	{
		const CGPDBAttOptCol *gpdb_att_opt_col_info = col_map_iterator.Value();
		const CGPDBAttInfo *gpdb_att_info =
			gpdb_att_opt_col_info->GetGPDBAttInfo();
		const COptColInfo *opt_col_info =
			gpdb_att_opt_col_info->GetOptColInfo();

		CGPDBAttInfo *gpdb_att_info_new = GPOS_NEW(mp)
			CGPDBAttInfo(gpdb_att_info->GetQueryLevel(),
						 gpdb_att_info->GetVarNo(), gpdb_att_info->GetAttNo());
		COptColInfo *opt_col_info_new = GPOS_NEW(mp) COptColInfo(
			opt_col_info->GetColId(),
			GPOS_NEW(mp)
				CWStringConst(mp, opt_col_info->GetOptColName()->GetBuffer()));
		gpdb_att_info_new->AddRef();
		CGPDBAttOptCol *gpdb_att_opt_col_new =
			GPOS_NEW(mp) CGPDBAttOptCol(gpdb_att_info_new, opt_col_info_new);

		// insert into hashmap
		BOOL result GPOS_ASSERTS_ONLY =
			var_colid_mapping->m_gpdb_att_opt_col_mapping->Insert(
				gpdb_att_info_new, gpdb_att_opt_col_new);
		GPOS_ASSERT(result);
	}

	return var_colid_mapping;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingVarColId::CopyRemapColId
//
//	@doc:
//		Create a copy of the mapping replacing the old column ids by new ones
//
//---------------------------------------------------------------------------
CMappingVarColId *
CMappingVarColId::CopyRemapColId(CMemoryPool *mp, ULongPtrArray *old_colids,
								 ULongPtrArray *new_colids) const
{
	GPOS_ASSERT(NULL != old_colids);
	GPOS_ASSERT(NULL != new_colids);
	GPOS_ASSERT(new_colids->Size() == old_colids->Size());

	// construct a mapping old cols -> new cols
	UlongToUlongMap *old_new_col_mapping =
		CTranslatorUtils::MakeNewToOldColMapping(mp, old_colids, new_colids);

	CMappingVarColId *var_colid_mapping = GPOS_NEW(mp) CMappingVarColId(mp);

	GPDBAttOptColHashMapIter col_map_iterator(this->m_gpdb_att_opt_col_mapping);
	while (col_map_iterator.Advance())
	{
		const CGPDBAttOptCol *gpdb_att_opt_col_info = col_map_iterator.Value();
		const CGPDBAttInfo *gpdb_att_info =
			gpdb_att_opt_col_info->GetGPDBAttInfo();
		const COptColInfo *opt_col_info =
			gpdb_att_opt_col_info->GetOptColInfo();

		CGPDBAttInfo *gpdb_att_info_new = GPOS_NEW(mp)
			CGPDBAttInfo(gpdb_att_info->GetQueryLevel(),
						 gpdb_att_info->GetVarNo(), gpdb_att_info->GetAttNo());
		ULONG colid = opt_col_info->GetColId();
		ULONG *new_colid = old_new_col_mapping->Find(&colid);
		if (NULL != new_colid)
		{
			colid = *new_colid;
		}

		COptColInfo *opt_col_info_new = GPOS_NEW(mp) COptColInfo(
			colid, GPOS_NEW(mp) CWStringConst(
					   mp, opt_col_info->GetOptColName()->GetBuffer()));
		gpdb_att_info_new->AddRef();
		CGPDBAttOptCol *gpdb_att_opt_col_new =
			GPOS_NEW(mp) CGPDBAttOptCol(gpdb_att_info_new, opt_col_info_new);

		BOOL result GPOS_ASSERTS_ONLY =
			var_colid_mapping->m_gpdb_att_opt_col_mapping->Insert(
				gpdb_att_info_new, gpdb_att_opt_col_new);
		GPOS_ASSERT(result);
	}

	old_new_col_mapping->Release();

	return var_colid_mapping;
}

// EOF
