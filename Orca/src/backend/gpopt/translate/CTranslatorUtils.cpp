//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTranslatorUtils.cpp
//
//	@doc:
//		Implementation of the utility methods for translating GPDB's
//		Query / PlannedStmt into DXL Tree
//
//	@test:
//
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/walkers.h"
#include "utils/guc.h"
#include "utils/rel.h"
}

#define GPDB_NEXTVAL 1574
#define GPDB_CURRVAL 1575
#define GPDB_SETVAL 1576

#include "gpos/attributes.h"
#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/gpdbwrappers.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/translate/CDXLTranslateContext.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/dxl/operators/CDXLDatumBool.h"
#include "naucrates/dxl/operators/CDXLDatumInt2.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLDatumInt8.h"
#include "naucrates/dxl/operators/CDXLDatumOid.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLSpoolInfo.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeInt2.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/IMDTypeOid.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpos;
using namespace gpopt;

extern bool optimizer_enable_master_only_queries;
extern bool optimizer_multilevel_partitioning;

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetIndexDescr
//
//	@doc:
//		Create a DXL index descriptor from an index MD id
//
//---------------------------------------------------------------------------
CDXLIndexDescr *
CTranslatorUtils::GetIndexDescr(CMemoryPool *mp, CMDAccessor *md_accessor,
								IMDId *mdid)
{
	const IMDIndex *index = md_accessor->RetrieveIndex(mdid);
	const CWStringConst *index_name = index->Mdname().GetMDName();
	CMDName *index_mdname = GPOS_NEW(mp) CMDName(mp, index_name);

	return GPOS_NEW(mp) CDXLIndexDescr(mdid, index_mdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetTableDescr
//
//	@doc:
//		Create a DXL table descriptor from a GPDB range table entry
//
//---------------------------------------------------------------------------
CDXLTableDescr *
CTranslatorUtils::GetTableDescr(CMemoryPool *mp, CMDAccessor *md_accessor,
								CIdGenerator *id_generator,
								const RangeTblEntry *rte,
								BOOL *is_distributed_table	// output
)
{
	// generate an MDId for the table desc.
	OID rel_oid = rte->relid;

#if 0
	if (gpdb::HasExternalPartition(rel_oid))
	{
		// fall back to the planner for queries with partition tables that has an external table in one of its leaf
		// partitions.
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Query over external partitions"));
	}
#endif

	CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(rel_oid);

	const IMDRelation *rel = md_accessor->RetrieveRel(mdid);

	// look up table name
	const CWStringConst *tablename = rel->Mdname().GetMDName();
	CMDName *table_mdname = GPOS_NEW(mp) CMDName(mp, tablename);

	CDXLTableDescr *table_descr = GPOS_NEW(mp) CDXLTableDescr(
		mp, mdid, table_mdname, rte->checkAsUser, rte->rellockmode);

	const ULONG len = rel->ColumnCount();

	IMDRelation::Ereldistrpolicy distribution_policy =
		rel->GetRelDistribution();

	if (NULL != is_distributed_table &&
		(IMDRelation::EreldistrHash == distribution_policy ||
		 IMDRelation::EreldistrRandom == distribution_policy ||
		 IMDRelation::EreldistrReplicated == distribution_policy))
	{
		*is_distributed_table = true;
	}
	else if (!optimizer_enable_master_only_queries &&
			 (IMDRelation::EreldistrMasterOnly == distribution_policy))
	{
		// fall back to the planner for queries on master-only table if they are disabled with Orca. This is due to
		// the fact that catalog tables (master-only) are not analyzed often and will result in Orca producing
		// inferior plans.

		GPOS_THROW_EXCEPTION(
			gpdxl::ExmaDXL,							 // major
			gpdxl::ExmiQuery2DXLUnsupportedFeature,	 // minor
			CException::
				ExsevDebug1,  // ulSeverityLevel mapped to GPDB severity level
			GPOS_WSZ_LIT("Queries on master-only tables"));
	}

	// add columns from md cache relation object to table descriptor
	for (ULONG ul = 0; ul < len; ul++)
	{
		const IMDColumn *md_col = rel->GetMdCol(ul);
		if (md_col->IsDropped())
		{
			continue;
		}

		CMDName *col = GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
		CMDIdGPDB *col_type = CMDIdGPDB::CastMdid(md_col->MdidType());
		col_type->AddRef();

		// create a column descriptor for the column
		CDXLColDescr *dxl_col_descr = GPOS_NEW(mp)
			CDXLColDescr(col, id_generator->next_id(), md_col->AttrNum(),
						 col_type, md_col->TypeModifier(), /* type_modifier */
						 false,							   /* fColDropped */
						 md_col->Length());
		table_descr->AddColumnDescr(dxl_col_descr);
	}

	return table_descr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::IsSirvFunc
//
//	@doc:
//		Check if the given function is a SIRV (single row volatile) that reads
//		or modifies SQL data
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::IsSirvFunc(CMemoryPool *mp, CMDAccessor *md_accessor,
							 OID func_oid)
{
	// we exempt the following 3 functions to avoid falling back to the planner
	// for DML on tables with sequences. The same exemption is also in the planner
	if (GPDB_NEXTVAL == func_oid || GPDB_CURRVAL == func_oid ||
		GPDB_SETVAL == func_oid)
	{
		return false;
	}

	CMDIdGPDB *mdid_func = GPOS_NEW(mp) CMDIdGPDB(func_oid);
	const IMDFunction *func = md_accessor->RetrieveFunc(mdid_func);

	BOOL is_sirv = (!func->ReturnsSet() &&
					IMDFunction::EfsVolatile == func->GetFuncStability());

	mdid_func->Release();

	return is_sirv;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::HasSubquery
//
//	@doc:
//		Check if the given tree contains a subquery
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::HasSubquery(Node *node)
{
	List *unsupported_list = ListMake1Int(T_SubLink);
	INT unsupported = gpdb::FindNodes(node, unsupported_list);
	gpdb::GPDBFree(unsupported_list);

	return (0 <= unsupported);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::ConvertToCDXLLogicalTVF
//
//	@doc:
//		Create a DXL logical TVF from a GPDB range table entry
//
//---------------------------------------------------------------------------
CDXLLogicalTVF *
CTranslatorUtils::ConvertToCDXLLogicalTVF(CMemoryPool *mp,
										  CMDAccessor *md_accessor,
										  CIdGenerator *id_generator,
										  const RangeTblEntry *rte)
{
	/*
	 * GPDB_94_MERGE_FIXME: RangeTblEntry for functions can now contain multiple function calls.
	 * ORCA isn't prepared for that yet. See upstream commit 784e762e88.
	 */
	if (list_length(rte->functions) != 1)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Multi-argument UNNEST() or TABLE()"));
	}
	/*
	 * GPDB_94_MERGE_FIXME: Does WITH ORDINALITY work? It was new in 9.4. Add a check here,
	 * if it doesn't, or remove this comment if it does.
	 */

	RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(rte->functions);
	FuncExpr *funcexpr = (FuncExpr *) rtfunc->funcexpr;
	GPOS_ASSERT(funcexpr);
	GPOS_ASSERT(IsA(funcexpr, FuncExpr));

	// In the planner, scalar functions that are volatile (SIRV) or read or modify SQL
	// data get patched into an InitPlan. This is not supported in the optimizer
	if (IsSirvFunc(mp, md_accessor, funcexpr->funcid))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("SIRV functions"));
	}

	// get function id
	CMDIdGPDB *mdid_func = GPOS_NEW(mp) CMDIdGPDB(funcexpr->funcid);
	CMDIdGPDB *mdid_return_type =
		GPOS_NEW(mp) CMDIdGPDB(funcexpr->funcresulttype);
	const IMDType *type = md_accessor->RetrieveType(mdid_return_type);

	// get function from MDcache
	const IMDFunction *func = md_accessor->RetrieveFunc(mdid_func);

	IMdIdArray *out_arg_types = func->OutputArgTypesMdidArray();

	CDXLColDescrArray *column_descrs = NULL;

	if (NULL != rtfunc->funccoltypes)
	{
		// function returns record - use col names and types from query
		column_descrs = GetColumnDescriptorsFromRecord(
			mp, id_generator, rte->eref->colnames, rtfunc->funccoltypes,
			rtfunc->funccoltypmods);
	}
	else if (type->IsComposite() && IMDId::IsValid(type->GetBaseRelMdid()))
	{
		// function returns a "table" type or a user defined type
		column_descrs = GetColumnDescriptorsFromComposite(mp, md_accessor,
														  id_generator, type);
	}
	else if (NULL != out_arg_types)
	{
		// function returns record - but output col types are defined in catalog
		out_arg_types->AddRef();
		if (ContainsPolymorphicTypes(out_arg_types))
		{
			// resolve polymorphic types (anyelement/anyarray) using the
			// argument types from the query
			List *arg_types = gpdb::GetFuncArgTypes(funcexpr->funcid);
			IMdIdArray *resolved_types =
				ResolvePolymorphicTypes(mp, out_arg_types, arg_types, funcexpr);
			out_arg_types->Release();
			out_arg_types = resolved_types;
		}

		column_descrs = GetColumnDescriptorsFromRecord(
			mp, id_generator, rte->eref->colnames, out_arg_types);
		out_arg_types->Release();
	}
	else
	{
		// function returns base type
		CMDName func_mdname = func->Mdname();
		// table valued functions don't describe the returned column type modifiers, hence the -1
		column_descrs =
			GetColumnDescriptorsFromBase(mp, id_generator, mdid_return_type,
										 default_type_modifier, &func_mdname);
	}

	CMDName *pmdfuncname = GPOS_NEW(mp) CMDName(mp, func->Mdname().GetMDName());

	CDXLLogicalTVF *tvf_dxl = GPOS_NEW(mp) CDXLLogicalTVF(
		mp, mdid_func, mdid_return_type, pmdfuncname, column_descrs);

	return tvf_dxl;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::ResolvePolymorphicTypes
//
//	@doc:
//		Resolve polymorphic types in the given array of type ids, replacing
//		them with the actual types obtained from the query
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorUtils::ResolvePolymorphicTypes(CMemoryPool *mp,
										  IMdIdArray *mdid_array,
										  List *arg_types_list,
										  FuncExpr *funcexpr)
{
	ULONG arg_index = 0;

	const ULONG num_arg_types = gpdb::ListLength(arg_types_list);
	const ULONG num_args_from_query = gpdb::ListLength(funcexpr->args);
	const ULONG num_return_args = mdid_array->Size();
	const ULONG num_args = num_arg_types < num_args_from_query
							   ? num_arg_types
							   : num_args_from_query;
	const ULONG total_args = num_args + num_return_args;

	OID arg_types[num_args];
	char arg_modes[total_args];

	// copy function argument types
	ListCell *arg_type = NULL;
	ForEach(arg_type, arg_types_list)
	{
		arg_types[arg_index] = lfirst_oid(arg_type);
		arg_modes[arg_index++] = PROARGMODE_IN;
	}

	// copy function return types
	for (ULONG ul = 0; ul < num_return_args; ul++)
	{
		IMDId *mdid = (*mdid_array)[ul];
		arg_types[arg_index] = CMDIdGPDB::CastMdid(mdid)->Oid();
		arg_modes[arg_index++] = PROARGMODE_TABLE;
	}

	if (!gpdb::ResolvePolymorphicArgType(total_args, arg_types, arg_modes,
										 funcexpr))
	{
		GPOS_RAISE(
			gpdxl::ExmaDXL, gpdxl::ExmiDXLUnrecognizedType,
			GPOS_WSZ_LIT(
				"could not determine actual argument/return type for polymorphic function"));
	}

	// generate a new array of mdids based on the resolved types
	IMdIdArray *resolved_types = GPOS_NEW(mp) IMdIdArray(mp);

	// get the resolved return types
	for (ULONG ul = num_args; ul < total_args; ul++)
	{
		IMDId *resolved_mdid = NULL;
		resolved_mdid = GPOS_NEW(mp) CMDIdGPDB(arg_types[ul]);
		resolved_types->Append(resolved_mdid);
	}

	return resolved_types;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::ContainsPolymorphicTypes
//
//	@doc:
//		Check if the given mdid array contains any of the polymorphic
//		types (ANYELEMENT, ANYARRAY, ANYENUM, ANYNONARRAY)
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::ContainsPolymorphicTypes(IMdIdArray *mdid_array)
{
	GPOS_ASSERT(NULL != mdid_array);
	const ULONG len = mdid_array->Size();
	for (ULONG ul = 0; ul < len; ul++)
	{
		IMDId *mdid_type = (*mdid_array)[ul];
		if (IsPolymorphicType(CMDIdGPDB::CastMdid(mdid_type)->Oid()))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColumnDescriptorsFromRecord
//
//	@doc:
//		Get column descriptors from a record type
//
//---------------------------------------------------------------------------
CDXLColDescrArray *
CTranslatorUtils::GetColumnDescriptorsFromRecord(CMemoryPool *mp,
												 CIdGenerator *id_generator,
												 List *col_names,
												 List *col_types,
												 List *col_type_modifiers)
{
	ListCell *col_name = NULL;
	ListCell *col_type = NULL;
	ListCell *col_type_modifier = NULL;

	ULONG ul = 0;
	CDXLColDescrArray *column_descrs = GPOS_NEW(mp) CDXLColDescrArray(mp);

	ForThree(col_name, col_names, col_type, col_types, col_type_modifier,
			 col_type_modifiers)
	{
		Value *value = (Value *) lfirst(col_name);
		Oid coltype = lfirst_oid(col_type);
		INT type_modifier = lfirst_int(col_type_modifier);

		CHAR *col_name_char_array = strVal(value);
		CWStringDynamic *column_name =
			CDXLUtils::CreateDynamicStringFromCharArray(mp,
														col_name_char_array);
		CMDName *col_mdname = GPOS_NEW(mp) CMDName(mp, column_name);
		GPOS_DELETE(column_name);

		IMDId *col_type = GPOS_NEW(mp) CMDIdGPDB(coltype);

		CDXLColDescr *dxl_col_descr = GPOS_NEW(mp) CDXLColDescr(
			col_mdname, id_generator->next_id(), INT(ul + 1) /* attno */,
			col_type, type_modifier, false /* fColDropped */
		);
		column_descrs->Append(dxl_col_descr);
		ul++;
	}

	return column_descrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColumnDescriptorsFromRecord
//
//	@doc:
//		Get column descriptors from a record type
//
//---------------------------------------------------------------------------
CDXLColDescrArray *
CTranslatorUtils::GetColumnDescriptorsFromRecord(CMemoryPool *mp,
												 CIdGenerator *id_generator,
												 List *col_names,
												 IMdIdArray *out_arg_types)
{
	GPOS_ASSERT(out_arg_types->Size() == (ULONG) gpdb::ListLength(col_names));
	ListCell *col_name = NULL;

	ULONG ul = 0;
	CDXLColDescrArray *column_descrs = GPOS_NEW(mp) CDXLColDescrArray(mp);

	ForEach(col_name, col_names)
	{
		Value *value = (Value *) lfirst(col_name);

		CHAR *col_name_char_array = strVal(value);
		CWStringDynamic *column_name =
			CDXLUtils::CreateDynamicStringFromCharArray(mp,
														col_name_char_array);
		CMDName *col_mdname = GPOS_NEW(mp) CMDName(mp, column_name);
		GPOS_DELETE(column_name);

		IMDId *col_type = (*out_arg_types)[ul];
		col_type->AddRef();

		// This function is only called to construct column descriptors for table-valued functions
		// which won't have type modifiers for columns of the returned table
		CDXLColDescr *dxl_col_descr = GPOS_NEW(mp) CDXLColDescr(
			col_mdname, id_generator->next_id(), INT(ul + 1) /* attno */,
			col_type, default_type_modifier, false /* fColDropped */
		);
		column_descrs->Append(dxl_col_descr);
		ul++;
	}

	return column_descrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColumnDescriptorsFromBase
//
//	@doc:
//		Get column descriptor from a base type
//
//---------------------------------------------------------------------------
CDXLColDescrArray *
CTranslatorUtils::GetColumnDescriptorsFromBase(CMemoryPool *mp,
											   CIdGenerator *id_generator,
											   IMDId *mdid_return_type,
											   INT type_modifier,
											   CMDName *pmdName)
{
	CDXLColDescrArray *column_descrs = GPOS_NEW(mp) CDXLColDescrArray(mp);

	mdid_return_type->AddRef();
	CMDName *col_mdname = GPOS_NEW(mp) CMDName(mp, pmdName->GetMDName());

	CDXLColDescr *dxl_col_descr = GPOS_NEW(mp)
		CDXLColDescr(col_mdname, id_generator->next_id(), INT(1) /* attno */,
					 mdid_return_type, type_modifier, /* type_modifier */
					 false							  /* fColDropped */
		);

	column_descrs->Append(dxl_col_descr);

	return column_descrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColumnDescriptorsFromComposite
//
//	@doc:
//		Get column descriptors from a composite type
//
//---------------------------------------------------------------------------
CDXLColDescrArray *
CTranslatorUtils::GetColumnDescriptorsFromComposite(CMemoryPool *mp,
													CMDAccessor *md_accessor,
													CIdGenerator *id_generator,
													const IMDType *type)
{
	CMDColumnArray *col_ptr_arr = ExpandCompositeType(mp, md_accessor, type);

	CDXLColDescrArray *column_descrs = GPOS_NEW(mp) CDXLColDescrArray(mp);

	for (ULONG ul = 0; ul < col_ptr_arr->Size(); ul++)
	{
		IMDColumn *md_col = (*col_ptr_arr)[ul];

		CMDName *col_mdname =
			GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
		IMDId *col_type = md_col->MdidType();

		col_type->AddRef();
		CDXLColDescr *dxl_col_descr = GPOS_NEW(mp) CDXLColDescr(
			col_mdname, id_generator->next_id(), INT(ul + 1) /* attno */,
			col_type, md_col->TypeModifier(), /* type_modifier */
			false							  /* fColDropped */
		);
		column_descrs->Append(dxl_col_descr);
	}

	col_ptr_arr->Release();

	return column_descrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::ExpandCompositeType
//
//	@doc:
//		Expand a composite type into an array of IMDColumns
//
//---------------------------------------------------------------------------
CMDColumnArray *
CTranslatorUtils::ExpandCompositeType(CMemoryPool *mp, CMDAccessor *md_accessor,
									  const IMDType *type)
{
	GPOS_ASSERT(NULL != type);
	GPOS_ASSERT(type->IsComposite());

	IMDId *rel_mdid = type->GetBaseRelMdid();
	const IMDRelation *rel = md_accessor->RetrieveRel(rel_mdid);
	GPOS_ASSERT(NULL != rel);

	CMDColumnArray *pdrgPmdcol = GPOS_NEW(mp) CMDColumnArray(mp);

	for (ULONG ul = 0; ul < rel->ColumnCount(); ul++)
	{
		CMDColumn *md_col = (CMDColumn *) rel->GetMdCol(ul);

		if (!md_col->IsSystemColumn())
		{
			md_col->AddRef();
			pdrgPmdcol->Append(md_col);
		}
	}

	return pdrgPmdcol;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::ConvertToDXLJoinType
//
//	@doc:
//		Translates the join type from its GPDB representation into the DXL one
//
//---------------------------------------------------------------------------
EdxlJoinType
CTranslatorUtils::ConvertToDXLJoinType(JoinType jt)
{
	EdxlJoinType join_type = EdxljtSentinel;

	switch (jt)
	{
		case JOIN_INNER:
			join_type = EdxljtInner;
			break;

		case JOIN_LEFT:
			join_type = EdxljtLeft;
			break;

		case JOIN_FULL:
			join_type = EdxljtFull;
			break;

		case JOIN_RIGHT:
			join_type = EdxljtRight;
			break;

		case JOIN_SEMI:
			join_type = EdxljtIn;
			break;

		case JOIN_ANTI:
			join_type = EdxljtLeftAntiSemijoin;
			break;

		case JOIN_LASJ_NOTIN:
			join_type = EdxljtLeftAntiSemijoinNotIn;
			break;

		default:
			GPOS_ASSERT(!"Unrecognized join type");
	}

	GPOS_ASSERT(EdxljtSentinel > join_type);

	return join_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::ConvertToDXLIndexScanDirection
//
//	@doc:
//		Translates the DXL index scan direction from GPDB representation
//
//---------------------------------------------------------------------------
EdxlIndexScanDirection
CTranslatorUtils::ConvertToDXLIndexScanDirection(ScanDirection sd)
{
	EdxlIndexScanDirection idx_scan_direction = EdxlisdSentinel;

	switch (sd)
	{
		case BackwardScanDirection:
			idx_scan_direction = EdxlisdBackward;
			break;

		case ForwardScanDirection:
			idx_scan_direction = EdxlisdForward;
			break;

		case NoMovementScanDirection:
			idx_scan_direction = EdxlisdNoMovement;
			break;

		default:
			GPOS_ASSERT(!"Unrecognized index scan direction");
	}

	GPOS_ASSERT(EdxlisdSentinel > idx_scan_direction);

	return idx_scan_direction;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColumnDescrAt
//
//	@doc:
//		Find the n-th col descr entry
//
//---------------------------------------------------------------------------
const CDXLColDescr *
CTranslatorUtils::GetColumnDescrAt(const CDXLTableDescr *table_descr, ULONG pos)
{
	GPOS_ASSERT(0 != pos);
	GPOS_ASSERT(pos < table_descr->Arity());

	return table_descr->GetColumnDescrAt(pos);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetSystemColName
//
//	@doc:
//		Return the name for the system attribute with the given attribute number.
//
//---------------------------------------------------------------------------
// GPDB_12_MERGE_FIXME: Can we get rid of this function? We should be able to get this info from pg_attribute
const CWStringConst *
CTranslatorUtils::GetSystemColName(AttrNumber attno)
{
	GPOS_ASSERT(FirstLowInvalidHeapAttributeNumber < attno && 0 > attno);

	switch (attno)
	{
		case SelfItemPointerAttributeNumber:
			return CDXLTokens::GetDXLTokenStr(EdxltokenCtidColName);

		case MinTransactionIdAttributeNumber:
			return CDXLTokens::GetDXLTokenStr(EdxltokenXminColName);

		case MinCommandIdAttributeNumber:
			return CDXLTokens::GetDXLTokenStr(EdxltokenCminColName);

		case MaxTransactionIdAttributeNumber:
			return CDXLTokens::GetDXLTokenStr(EdxltokenXmaxColName);

		case MaxCommandIdAttributeNumber:
			return CDXLTokens::GetDXLTokenStr(EdxltokenCmaxColName);

		case TableOidAttributeNumber:
			return CDXLTokens::GetDXLTokenStr(EdxltokenTableOidColName);

		case GpSegmentIdAttributeNumber:
			return CDXLTokens::GetDXLTokenStr(EdxltokenGpSegmentIdColName);

		default:
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiPlStmt2DXLConversion,
					   GPOS_WSZ_LIT("Invalid attribute number"));
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetSystemColType
//
//	@doc:
//		Return the type id for the system attribute with the given attribute number.
//
//---------------------------------------------------------------------------
// GPDB_12_MERGE_FIXME: Can we get rid of this function? We should be able to get this info from pg_attribute
CMDIdGPDB *
CTranslatorUtils::GetSystemColType(CMemoryPool *mp, AttrNumber attno)
{
	GPOS_ASSERT(FirstLowInvalidHeapAttributeNumber < attno && 0 > attno);

	switch (attno)
	{
		case SelfItemPointerAttributeNumber:
			// tid type
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_TID);

		case TableOidAttributeNumber:
			// OID type
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_OID);

		case MinTransactionIdAttributeNumber:
		case MaxTransactionIdAttributeNumber:
			// xid type
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_XID);

		case MinCommandIdAttributeNumber:
		case MaxCommandIdAttributeNumber:
			// cid type
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_CID);

		case GpSegmentIdAttributeNumber:
			// int4
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4);

		default:
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiPlStmt2DXLConversion,
					   GPOS_WSZ_LIT("Invalid attribute number"));
			return NULL;
	}
}


// Returns the length for the system column with given attno number
// GPDB_12_MERGE_FIXME: Can we get rid of this function? We should be able to get this info from pg_attribute
const ULONG
CTranslatorUtils::GetSystemColLength(AttrNumber attno)
{
	GPOS_ASSERT(FirstLowInvalidHeapAttributeNumber < attno && 0 > attno);

	switch (attno)
	{
		case SelfItemPointerAttributeNumber:
			// tid type
			return 6;

		case TableOidAttributeNumber:
			// OID type

		case MinTransactionIdAttributeNumber:
		case MaxTransactionIdAttributeNumber:
			// xid type

		case MinCommandIdAttributeNumber:
		case MaxCommandIdAttributeNumber:
			// cid type

		case GpSegmentIdAttributeNumber:
			// int4
			return 4;

		default:
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiPlStmt2DXLConversion,
					   GPOS_WSZ_LIT("Invalid attribute number"));
			return gpos::ulong_max;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetScanDirection
//
//	@doc:
//		Return the GPDB specific scan direction from its corresponding DXL
//		representation
//
//---------------------------------------------------------------------------
ScanDirection
CTranslatorUtils::GetScanDirection(EdxlIndexScanDirection idx_scan_direction)
{
	if (EdxlisdBackward == idx_scan_direction)
	{
		return BackwardScanDirection;
	}

	if (EdxlisdForward == idx_scan_direction)
	{
		return ForwardScanDirection;
	}

	return NoMovementScanDirection;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::OidCmpOperator
//
//	@doc:
//		Extract comparison operator from an OpExpr, ScalarArrayOpExpr or RowCompareExpr
//
//---------------------------------------------------------------------------
OID
CTranslatorUtils::OidCmpOperator(Expr *expr)
{
	GPOS_ASSERT(IsA(expr, OpExpr) || IsA(expr, ScalarArrayOpExpr) ||
				IsA(expr, RowCompareExpr));

	switch (expr->type)
	{
		case T_OpExpr:
			return ((OpExpr *) expr)->opno;

		case T_ScalarArrayOpExpr:
			return ((ScalarArrayOpExpr *) expr)->opno;

		case T_RowCompareExpr:
			return LInitialOID(((RowCompareExpr *) expr)->opnos);

		default:
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiPlStmt2DXLConversion,
					   GPOS_WSZ_LIT("Unsupported comparison"));
			return InvalidOid;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetOpFamilyForIndexQual
//
//	@doc:
//		Extract comparison operator family for the given index column
//
//---------------------------------------------------------------------------
OID
CTranslatorUtils::GetOpFamilyForIndexQual(INT attno, OID index_oid)
{
	gpdb::RelationWrapper rel = gpdb::GetRelation(index_oid);
	GPOS_ASSERT(rel);
	GPOS_ASSERT(attno <= rel->rd_index->indnatts);

	OID op_family_oid = rel->rd_opfamily[attno - 1];

	return op_family_oid;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetSetOpType
//
//	@doc:
//		Return the DXL representation of the set operation
//
//---------------------------------------------------------------------------
EdxlSetOpType
CTranslatorUtils::GetSetOpType(SetOperation setop, BOOL is_all)
{
	if (SETOP_UNION == setop && is_all)
	{
		return EdxlsetopUnionAll;
	}

	if (SETOP_INTERSECT == setop && is_all)
	{
		return EdxlsetopIntersectAll;
	}

	if (SETOP_EXCEPT == setop && is_all)
	{
		return EdxlsetopDifferenceAll;
	}

	if (SETOP_UNION == setop)
	{
		return EdxlsetopUnion;
	}

	if (SETOP_INTERSECT == setop)
	{
		return EdxlsetopIntersect;
	}

	if (SETOP_EXCEPT == setop)
	{
		return EdxlsetopDifference;
	}

	GPOS_ASSERT(!"Unrecognized set operator type");
	return EdxlsetopSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetGroupingColidArray
//
//	@doc:
//		Construct a dynamic array of column ids for the given set of grouping
// 		col attnos
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorUtils::GetGroupingColidArray(
	CMemoryPool *mp, CBitSet *group_by_cols,
	IntToUlongMap *sort_group_cols_to_colid_map)
{
	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);

	if (NULL != group_by_cols)
	{
		CBitSetIter bsi(*group_by_cols);

		while (bsi.Advance())
		{
			const ULONG colid =
				GetColId(bsi.Bit(), sort_group_cols_to_colid_map);
			colids->Append(GPOS_NEW(mp) ULONG(colid));
		}
	}

	return colids;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColumnAttnosForGroupBy
//
//	@doc:
//		Construct a dynamic array of sets of column attnos corresponding to the
// 		group by clause
//
//---------------------------------------------------------------------------
CBitSetArray *
CTranslatorUtils::GetColumnAttnosForGroupBy(
	CMemoryPool *mp, List *group_clause_list, List *grouping_set_list,
	ULONG num_cols,
	UlongToUlongMap *
		group_col_pos,	// mapping of grouping col positions to SortGroupRef ids
	CBitSet *group_cols	 // existing uniqueue grouping columns
)
{
	GPOS_ASSERT(NULL != group_col_pos);

	if (NIL == grouping_set_list)
	{
		// simple group by
		CBitSet *col_attnos = CreateAttnoSetForGroupingSet(
			mp, group_clause_list, num_cols, group_col_pos, group_cols,
			true /* use_group_clause */);
		CBitSetArray *col_attnos_arr = GPOS_NEW(mp) CBitSetArray(mp);
		col_attnos_arr->Append(col_attnos);
		return col_attnos_arr;
	}

	GPOS_ASSERT(0 < gpdb::ListLength(grouping_set_list));

	if (1 < gpdb::ListLength(grouping_set_list))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Multiple grouping sets specifications"));
	}

	Node *node = (Node *) LInitial(grouping_set_list);
	GPOS_ASSERT(NULL != node && IsA(node, GroupingSet));
	GroupingSet *grouping_set = (GroupingSet *) node;
	CBitSetArray *col_attnos_arr = NULL;

	switch (grouping_set->kind)
	{
		case GROUPING_SET_EMPTY:
		{
			col_attnos_arr = GPOS_NEW(mp) CBitSetArray(mp);
			CBitSet *bset = GPOS_NEW(mp) CBitSet(mp);
			col_attnos_arr->Append(bset);
			break;
		}
		case GROUPING_SET_ROLLUP:
		{
			col_attnos_arr = CreateGroupingSetsForRollup(
				mp, grouping_set, num_cols, group_cols, group_col_pos);
			break;
		}
		case GROUPING_SET_CUBE:
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("Cube"));
		}
		case GROUPING_SET_SETS:
		{
			col_attnos_arr = CreateGroupingSetsForSets(
				mp, grouping_set, num_cols, group_cols, group_col_pos);
			break;
		}
		case GROUPING_SET_SIMPLE:
		default:
		{
			/* can't happen */
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLError,
					   GPOS_WSZ_LIT("Unrecognized grouping set kind"));
		}
	}

	return col_attnos_arr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateGroupingSetsForSets
//
//	@doc:
//		Construct a dynamic array of sets of column attnos for a grouping sets
//		subclause
//
//---------------------------------------------------------------------------
CBitSetArray *
CTranslatorUtils::CreateGroupingSetsForSets(CMemoryPool *mp,
											const GroupingSet *grouping_set,
											ULONG num_cols, CBitSet *group_cols,
											UlongToUlongMap *group_col_pos)
{
	GPOS_ASSERT(NULL != grouping_set);
	GPOS_ASSERT(grouping_set->kind == GROUPING_SET_SETS);
	CBitSetArray *col_attnos_arr = GPOS_NEW(mp) CBitSetArray(mp);

	ListCell *cell = NULL;
	ForEach(cell, grouping_set->content)
	{
		Node *n = (Node *) lfirst(cell);
		GPOS_ASSERT(IsA(n, GroupingSet));
		GroupingSet *gs_current = (GroupingSet *) n;

		CBitSet *bset = NULL;
		switch (gs_current->kind)
		{
			case GROUPING_SET_EMPTY:
				bset = GPOS_NEW(mp) CBitSet(mp, num_cols);
				break;
			case GROUPING_SET_SIMPLE:
				bset = CreateAttnoSetForGroupingSet(
					mp, gs_current->content, num_cols, group_col_pos,
					group_cols, false /* use_group_clause */);
				break;
			default:
				GPOS_RAISE(ExmaDXL, ExmiQuery2DXLUnsupportedFeature,
						   GPOS_WSZ_LIT("nested grouping set"));
		}
		col_attnos_arr->Append(bset);
	}
	return col_attnos_arr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateGroupingSetsForRollup
//
//	@doc:
//		Construct a dynamic array of sets of column attnos for a rollup
//
//---------------------------------------------------------------------------
CBitSetArray *
CTranslatorUtils::CreateGroupingSetsForRollup(CMemoryPool *mp,
											  const GroupingSet *grouping_set,
											  ULONG num_cols,
											  CBitSet *group_cols,
											  UlongToUlongMap *group_col_pos)
{
	GPOS_ASSERT(NULL != grouping_set);
	GPOS_ASSERT(grouping_set->kind == GROUPING_SET_ROLLUP);
	CBitSetArray *col_attnos_arr = GPOS_NEW(mp) CBitSetArray(mp);
	ListCell *lc = NULL;
	CBitSet *current_result = GPOS_NEW(mp) CBitSet(mp);
	ForEach(lc, grouping_set->content)
	{
		GroupingSet *gs_current = (GroupingSet *) lfirst(lc);
		GPOS_ASSERT(gs_current->kind == GROUPING_SET_SIMPLE);

		CBitSet *bset = CreateAttnoSetForGroupingSet(
			mp, gs_current->content, num_cols, group_col_pos, group_cols,
			false /* use_group_clause */);
		current_result->Union(bset);
		bset->Release();
		col_attnos_arr->Append(GPOS_NEW(mp) CBitSet(mp, *current_result));
	}
	// add an empty set
	col_attnos_arr->Append(GPOS_NEW(mp) CBitSet(mp));
	current_result->Release();
	return col_attnos_arr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateAttnoSetForGroupingSet
//
//	@doc:
//		Construct a set of column attnos corresponding to a single grouping set
//		from either a simple GROUP BY or a list of grouping sets
//
//---------------------------------------------------------------------------
CBitSet *
CTranslatorUtils::CreateAttnoSetForGroupingSet(
	CMemoryPool *mp, List *group_elems, ULONG num_cols,
	UlongToUlongMap *
		group_col_pos,	// mapping of grouping col positions to SortGroupRef ids
	CBitSet *group_cols,  // existing grouping columns
	bool use_group_clause)
{
	GPOS_ASSERT(NIL != group_elems);
	GPOS_ASSERT(0 < gpdb::ListLength(group_elems));

	CBitSet *bs = GPOS_NEW(mp) CBitSet(mp, num_cols);

	ListCell *lc = NULL;
	ForEach(lc, group_elems)
	{
		ULONG sort_group_ref;
		if (use_group_clause)
		{
			Node *elem_node = (Node *) lfirst(lc);
			GPOS_ASSERT(NULL != elem_node);
			GPOS_ASSERT(IsA(elem_node, SortGroupClause));
			sort_group_ref = ((SortGroupClause *) elem_node)->tleSortGroupRef;
		}
		else
		{
			sort_group_ref = lfirst_int(lc);
		}
		bs->ExchangeSet(sort_group_ref);
		UpdateGrpColMapping(mp, group_col_pos, group_cols, sort_group_ref);
	}

	return bs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GenerateColIds
//
//	@doc:
//		Construct an array of DXL column identifiers for a target list
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorUtils::GenerateColIds(
	CMemoryPool *mp, List *target_list, IMdIdArray *input_mdid_arr,
	ULongPtrArray *input_colids,
	BOOL *
		is_outer_ref,  // array of flags indicating if input columns are outer references
	CIdGenerator *colid_generator)
{
	GPOS_ASSERT(NULL != target_list);
	GPOS_ASSERT(NULL != input_mdid_arr);
	GPOS_ASSERT(NULL != input_colids);
	GPOS_ASSERT(NULL != is_outer_ref);
	GPOS_ASSERT(NULL != colid_generator);

	GPOS_ASSERT(input_mdid_arr->Size() == input_colids->Size());

	ULONG col_pos = 0;
	ListCell *target_entry_cell = NULL;
	ULongPtrArray *colid_array = GPOS_NEW(mp) ULongPtrArray(mp);

	ForEach(target_entry_cell, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(target_entry_cell);
		GPOS_ASSERT(NULL != target_entry->expr);

		OID expr_type_oid = gpdb::ExprType((Node *) target_entry->expr);
		if (!target_entry->resjunk)
		{
			ULONG colid = gpos::ulong_max;
			IMDId *mdid = (*input_mdid_arr)[col_pos];
			if (CMDIdGPDB::CastMdid(mdid)->Oid() != expr_type_oid ||
				is_outer_ref[col_pos])
			{
				// generate a new column when:
				//  (1) the type of input column does not match that of the output column, or
				//  (2) input column is an outer reference
				colid = colid_generator->next_id();
			}
			else
			{
				// use the column identifier of the input
				colid = *(*input_colids)[col_pos];
			}
			GPOS_ASSERT(gpos::ulong_max != colid);

			colid_array->Append(GPOS_NEW(mp) ULONG(colid));

			col_pos++;
		}
	}

	return colid_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FixUnknownTypeConstant
//
//	@doc:
//		If the query has constant of unknown type, then return a copy of the
//		query with all constants of unknown type being coerced to the common data type
//		of the output target list; otherwise return the original query
//---------------------------------------------------------------------------
Query *
CTranslatorUtils::FixUnknownTypeConstant(Query *old_query,
										 List *output_target_list)
{
	GPOS_ASSERT(NULL != old_query);
	GPOS_ASSERT(NULL != output_target_list);

	Query *new_query = NULL;

	ULONG pos = 0;
	ULONG col_pos = 0;
	ListCell *target_entry_cell = NULL;
	ForEach(target_entry_cell, old_query->targetList)
	{
		TargetEntry *old_target_entry =
			(TargetEntry *) lfirst(target_entry_cell);
		GPOS_ASSERT(NULL != old_target_entry->expr);

		if (!old_target_entry->resjunk)
		{
			if (IsA(old_target_entry->expr, Const) &&
				(GPDB_UNKNOWN ==
				 gpdb::ExprType((Node *) old_target_entry->expr)))
			{
				if (NULL == new_query)
				{
					new_query = (Query *) gpdb::CopyObject(
						const_cast<Query *>(old_query));
				}

				TargetEntry *new_target_entry =
					(TargetEntry *) gpdb::ListNth(new_query->targetList, pos);
				GPOS_ASSERT(old_target_entry->resno == new_target_entry->resno);
				// implicitly cast the unknown constants to the target data type
				OID target_type_oid =
					GetTargetListReturnTypeOid(output_target_list, col_pos);
				GPOS_ASSERT(InvalidOid != target_type_oid);
				Node *old_node = (Node *) new_target_entry->expr;
				new_target_entry->expr = (Expr *) gpdb::CoerceToCommonType(
					NULL, /* pstate */
					(Node *) old_node, target_type_oid,
					"UNION/INTERSECT/EXCEPT");

				gpdb::GPDBFree(old_node);
			}
			col_pos++;
		}

		pos++;
	}

	if (NULL == new_query)
	{
		return old_query;
	}

	gpdb::GPDBFree(old_query);

	return new_query;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetTargetListReturnTypeOid
//
//	@doc:
//		Return the type of the nth non-resjunked target list entry
//
//---------------------------------------------------------------------------
OID
CTranslatorUtils::GetTargetListReturnTypeOid(List *target_list, ULONG col_pos)
{
	ULONG col_idx = 0;
	ListCell *target_entry_cell = NULL;

	ForEach(target_entry_cell, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(target_entry_cell);
		GPOS_ASSERT(NULL != target_entry->expr);

		if (!target_entry->resjunk)
		{
			if (col_idx == col_pos)
			{
				return gpdb::ExprType((Node *) target_entry->expr);
			}

			col_idx++;
		}
	}

	return InvalidOid;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetDXLColumnDescrArray
//
//	@doc:
//		Construct an array of DXL column descriptors for a target list using the
// 		column ids in the given array
//
//---------------------------------------------------------------------------
CDXLColDescrArray *
CTranslatorUtils::GetDXLColumnDescrArray(CMemoryPool *mp, List *target_list,
										 ULongPtrArray *colids,
										 BOOL keep_res_junked)
{
	GPOS_ASSERT(NULL != colids);

	ListCell *target_entry_cell = NULL;
	CDXLColDescrArray *dxl_col_descrs = GPOS_NEW(mp) CDXLColDescrArray(mp);
	ULONG ul = 0;
	ForEach(target_entry_cell, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(target_entry_cell);

		if (target_entry->resjunk && !keep_res_junked)
		{
			continue;
		}

		ULONG colid = *(*colids)[ul];
		CDXLColDescr *dxl_col_descr =
			GetColumnDescrAt(mp, target_entry, colid, ul + 1 /*pos*/);
		dxl_col_descrs->Append(dxl_col_descr);
		ul++;
	}

	GPOS_ASSERT(dxl_col_descrs->Size() == colids->Size());

	return dxl_col_descrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetPosInTargetList
//
//	@doc:
//		Return the positions of the target list entries included in the output
//		target list
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorUtils::GetPosInTargetList(CMemoryPool *mp, List *target_list,
									 BOOL keep_res_junked)
{
	GPOS_ASSERT(NULL != target_list);

	ListCell *target_entry_cell = NULL;
	ULongPtrArray *positions = GPOS_NEW(mp) ULongPtrArray(mp);
	ULONG ul = 0;
	ForEach(target_entry_cell, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(target_entry_cell);

		if (target_entry->resjunk && !keep_res_junked)
		{
			continue;
		}

		positions->Append(GPOS_NEW(mp) ULONG(ul));
		ul++;
	}

	return positions;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColumnDescrAt
//
//	@doc:
//		Construct a column descriptor from the given target entry and column
//		identifier
//---------------------------------------------------------------------------
CDXLColDescr *
CTranslatorUtils::GetColumnDescrAt(CMemoryPool *mp, TargetEntry *target_entry,
								   ULONG colid, ULONG pos)
{
	GPOS_ASSERT(NULL != target_entry);
	GPOS_ASSERT(gpos::ulong_max != colid);

	CMDName *mdname = NULL;
	if (NULL == target_entry->resname)
	{
		CWStringConst unnamed_col(GPOS_WSZ_LIT("?column?"));
		mdname = GPOS_NEW(mp) CMDName(mp, &unnamed_col);
	}
	else
	{
		CWStringDynamic *alias = CDXLUtils::CreateDynamicStringFromCharArray(
			mp, target_entry->resname);
		mdname = GPOS_NEW(mp) CMDName(mp, alias);
		// CName constructor copies string
		GPOS_DELETE(alias);
	}

	// create a column descriptor
	OID type_oid = gpdb::ExprType((Node *) target_entry->expr);
	INT type_modifier = gpdb::ExprTypeMod((Node *) target_entry->expr);
	CMDIdGPDB *col_type = GPOS_NEW(mp) CMDIdGPDB(type_oid);
	CDXLColDescr *dxl_col_descr =
		GPOS_NEW(mp) CDXLColDescr(mdname, colid, pos,	   /* attno */
								  col_type, type_modifier, /* type_modifier */
								  false					   /* fColDropped */
		);

	return dxl_col_descr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateDummyProjectElem
//
//	@doc:
//		Create a dummy project element to rename the input column identifier
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::CreateDummyProjectElem(CMemoryPool *mp, ULONG colid_input,
										 ULONG colid_output,
										 CDXLColDescr *dxl_col_descr)
{
	CMDIdGPDB *original_mdid = CMDIdGPDB::CastMdid(dxl_col_descr->MdidType());
	CMDIdGPDB *copy_mdid = GPOS_NEW(mp)
		CMDIdGPDB(original_mdid->Oid(), original_mdid->VersionMajor(),
				  original_mdid->VersionMinor());

	// create a column reference for the scalar identifier to be casted
	CMDName *mdname =
		GPOS_NEW(mp) CMDName(mp, dxl_col_descr->MdName()->GetMDName());
	CDXLColRef *dxl_colref = GPOS_NEW(mp) CDXLColRef(
		mdname, colid_input, copy_mdid, dxl_col_descr->TypeModifier());
	CDXLScalarIdent *dxl_scalar_ident =
		GPOS_NEW(mp) CDXLScalarIdent(mp, dxl_colref);

	CDXLNode *dxl_project_element = GPOS_NEW(mp) CDXLNode(
		mp,
		GPOS_NEW(mp) CDXLScalarProjElem(
			mp, colid_output,
			GPOS_NEW(mp) CMDName(mp, dxl_col_descr->MdName()->GetMDName())),
		GPOS_NEW(mp) CDXLNode(mp, dxl_scalar_ident));

	return dxl_project_element;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetOutputColIdsArray
//
//	@doc:
//		Construct an array of colids for the given target list
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorUtils::GetOutputColIdsArray(CMemoryPool *mp, List *target_list,
									   IntToUlongMap *attno_to_colid_map)
{
	GPOS_ASSERT(NULL != attno_to_colid_map);

	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);

	ListCell *target_entry_cell = NULL;
	ForEach(target_entry_cell, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(target_entry_cell);
		ULONG resno = (ULONG) target_entry->resno;
		INT attno = (INT) target_entry->resno;
		const ULONG *ul = attno_to_colid_map->Find(&attno);

		if (NULL == ul)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLAttributeNotFound,
					   resno);
		}

		colids->Append(GPOS_NEW(mp) ULONG(*ul));
	}

	return colids;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColId
//
//	@doc:
//		Return the corresponding ColId for the given index into the target list
//
//---------------------------------------------------------------------------
ULONG
CTranslatorUtils::GetColId(INT index, IntToUlongMap *colid_map)
{
	GPOS_ASSERT(0 < index);

	const ULONG *ul = colid_map->Find(&index);

	if (NULL == ul)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLAttributeNotFound,
				   index);
	}

	return *ul;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetColId
//
//	@doc:
//		Return the corresponding ColId for the given varno, varattno and querylevel
//
//---------------------------------------------------------------------------
ULONG
CTranslatorUtils::GetColId(ULONG query_level, INT varno, INT var_attno,
						   IMDId *mdid, CMappingVarColId *var_colid_mapping)
{
	OID oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	Var *var = gpdb::MakeVar(varno, var_attno, oid, -1, 0);
	ULONG colid = var_colid_mapping->GetColId(query_level, var, EpspotNone);
	gpdb::GPDBFree(var);

	return colid;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetWindowSpecTargetEntry
//
//	@doc:
//		Extract a matching target entry that is a window spec
//
//---------------------------------------------------------------------------
TargetEntry *
CTranslatorUtils::GetWindowSpecTargetEntry(Node *node, List *window_clause_list,
										   List *target_list)
{
	GPOS_ASSERT(NULL != node);
	List *target_list_subset =
		gpdb::FindMatchingMembersInTargetList(node, target_list);

	ListCell *target_entry_cell = NULL;
	ForEach(target_entry_cell, target_list_subset)
	{
		TargetEntry *cur_target_entry =
			(TargetEntry *) lfirst(target_entry_cell);
		if (IsReferencedInWindowSpec(cur_target_entry, window_clause_list))
		{
			gpdb::GPDBFree(target_list_subset);
			return cur_target_entry;
		}
	}

	if (NIL != target_list_subset)
	{
		gpdb::GPDBFree(target_list_subset);
	}
	return NULL;
}


//---------------------------------------------------------------------------
// CTranslatorUtils::IsReferencedInWindowSpec
// Check if the TargetEntry is a used in a window specification
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::IsReferencedInWindowSpec(const TargetEntry *target_entry,
										   List *window_clause_list)
{
	ListCell *window_clause_cell;
	ForEach(window_clause_cell, window_clause_list)
	{
		WindowClause *window_clause =
			(WindowClause *) lfirst(window_clause_cell);
		if (IsSortingColumn(target_entry, window_clause->orderClause) ||
			IsSortingColumn(target_entry, window_clause->partitionClause))
		{
			return true;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateDXLProjElemFromInt8Const
//
//	@doc:
// 		Construct a scalar const value expression for the given BIGINT value
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::CreateDXLProjElemFromInt8Const(CMemoryPool *mp,
												 CMDAccessor *md_accessor,
												 INT val)
{
	GPOS_ASSERT(NULL != mp);
	const IMDTypeInt8 *md_type_int8 = md_accessor->PtMDType<IMDTypeInt8>();
	md_type_int8->MDId()->AddRef();

	CDXLDatumInt8 *datum_dxl = GPOS_NEW(mp)
		CDXLDatumInt8(mp, md_type_int8->MDId(), false /*fConstNull*/, val);

	CDXLScalarConstValue *dxl_scalar_const =
		GPOS_NEW(mp) CDXLScalarConstValue(mp, datum_dxl);

	return GPOS_NEW(mp) CDXLNode(mp, dxl_scalar_const);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::IsSortingColumn
//
//	@doc:
//		Check if the TargetEntry is a sorting column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::IsSortingColumn(const TargetEntry *target_entry,
								  List *sort_clause_list)
{
	ListCell *sort_clause_cell = NULL;
	ForEach(sort_clause_cell, sort_clause_list)
	{
		Node *sort_clause = (Node *) lfirst(sort_clause_cell);
		if (IsA(sort_clause, SortGroupClause) &&
			target_entry->ressortgroupref ==
				((SortGroupClause *) sort_clause)->tleSortGroupRef)
		{
			return true;
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetGroupingColumnTargetEntry
//
//	@doc:
//		Extract a matching target entry that is a grouping column
//---------------------------------------------------------------------------
TargetEntry *
CTranslatorUtils::GetGroupingColumnTargetEntry(Node *node, List *group_clause,
											   List *target_list)
{
	GPOS_ASSERT(NULL != node);
	List *target_list_subset =
		gpdb::FindMatchingMembersInTargetList(node, target_list);

	ListCell *target_entry_cell = NULL;
	ForEach(target_entry_cell, target_list_subset)
	{
		TargetEntry *next_target_entry =
			(TargetEntry *) lfirst(target_entry_cell);
		if (IsGroupingColumn(next_target_entry, group_clause))
		{
			gpdb::GPDBFree(target_list_subset);
			return next_target_entry;
		}
	}

	if (NIL != target_list_subset)
	{
		gpdb::GPDBFree(target_list_subset);
	}
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::IsGroupingColumn
//
//	@doc:
//		Check if the expression has a matching target entry that is a grouping column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::IsGroupingColumn(Node *node, List *group_clause,
								   List *target_list)
{
	GPOS_ASSERT(NULL != node);

	TargetEntry *grouping_col_target_etnry =
		GetGroupingColumnTargetEntry(node, group_clause, target_list);

	return (NULL != grouping_col_target_etnry);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::IsGroupingColumn
//
//	@doc:
//		Check if the TargetEntry is a grouping column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::IsGroupingColumn(const TargetEntry *target_entry,
								   List *group_clause)
{
	ListCell *group_clause_cell = NULL;
	ForEach(group_clause_cell, group_clause)
	{
		Node *group_clause_node = (Node *) lfirst(group_clause_cell);

		GPOS_ASSERT(NULL != group_clause_node);
		GPOS_ASSERT(IsA(group_clause_node, SortGroupClause));

		if (IsGroupingColumn(target_entry,
							 (SortGroupClause *) group_clause_node))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::IsGroupingColumn
//
//	@doc:
//		Check if the TargetEntry is a grouping column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::IsGroupingColumn(const TargetEntry *target_entry,
								   const SortGroupClause *grouping_clause)
{
	GPOS_ASSERT(NULL != grouping_clause);

	return (target_entry->ressortgroupref == grouping_clause->tleSortGroupRef);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PlAttnosFromColids
//
//	@doc:
//		Translate an array of colids to a list of attribute numbers using
//		the mappings in the provided context
//---------------------------------------------------------------------------
List *
CTranslatorUtils::ConvertColidToAttnos(ULongPtrArray *colids,
									   CDXLTranslateContext *translate_ctxt)
{
	GPOS_ASSERT(NULL != colids);
	GPOS_ASSERT(NULL != translate_ctxt);

	List *result = NIL;

	const ULONG length = colids->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ULONG colid = *((*colids)[ul]);
		const TargetEntry *target_entry = translate_ctxt->GetTargetEntry(colid);
		GPOS_ASSERT(NULL != target_entry);
		result = gpdb::LAppendInt(result, target_entry->resno);
	}

	return result;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetLongFromStr
//
//	@doc:
//		Parses a long integer value from a string
//
//---------------------------------------------------------------------------
LINT
CTranslatorUtils::GetLongFromStr(const CWStringBase *wcstr)
{
	CHAR *str = CreateMultiByteCharStringFromWCString(wcstr->GetBuffer());
	CHAR *end = NULL;
	return gpos::clib::Strtol(str, &end, 10);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetIntFromStr
//
//	@doc:
//		Parses an integer value from a string
//
//---------------------------------------------------------------------------
INT
CTranslatorUtils::GetIntFromStr(const CWStringBase *wcstr)
{
	return (INT) GetLongFromStr(wcstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateMultiByteCharStringFromWCString
//
//	@doc:
//		Converts a wide character string into a character array
//
//---------------------------------------------------------------------------
CHAR *
CTranslatorUtils::CreateMultiByteCharStringFromWCString(const WCHAR *wcstr)
{
	GPOS_ASSERT(NULL != wcstr);

	ULONG max_len = GPOS_WSZ_LENGTH(wcstr) * GPOS_SIZEOF(WCHAR) + 1;
	CHAR *str = (CHAR *) gpdb::GPDBAlloc(max_len);
#ifdef GPOS_DEBUG
	LINT li = (INT)
#endif
		clib::Wcstombs(str, const_cast<WCHAR *>(wcstr), max_len);
	GPOS_ASSERT(0 <= li);

	str[max_len - 1] = '\0';

	return str;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::MakeNewToOldColMapping
//
//	@doc:
//		Create a mapping from old columns to the corresponding new column
//
//---------------------------------------------------------------------------
UlongToUlongMap *
CTranslatorUtils::MakeNewToOldColMapping(CMemoryPool *mp,
										 ULongPtrArray *old_colids,
										 ULongPtrArray *new_colids)
{
	GPOS_ASSERT(NULL != old_colids);
	GPOS_ASSERT(NULL != new_colids);
	GPOS_ASSERT(new_colids->Size() == old_colids->Size());

	UlongToUlongMap *old_new_col_mapping = GPOS_NEW(mp) UlongToUlongMap(mp);
	const ULONG num_cols = old_colids->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		ULONG old_colid = *((*old_colids)[ul]);
		ULONG new_colid = *((*new_colids)[ul]);
#ifdef GPOS_DEBUG
		BOOL result =
#endif	// GPOS_DEBUG
			old_new_col_mapping->Insert(GPOS_NEW(mp) ULONG(old_colid),
										GPOS_NEW(mp) ULONG(new_colid));
		GPOS_ASSERT(result);
	}

	return old_new_col_mapping;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::IsDuplicateSensitiveMotion
//
//	@doc:
//		Is this a motion sensitive to duplicates
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::IsDuplicateSensitiveMotion(CDXLPhysicalMotion *dxl_motion)
{
	Edxlopid dxl_opid = dxl_motion->GetDXLOperator();

	if (EdxlopPhysicalMotionRedistribute == dxl_opid)
	{
		return CDXLPhysicalRedistributeMotion::Cast(dxl_motion)
			->IsDuplicateSensitive();
	}

	if (EdxlopPhysicalMotionRandom == dxl_opid)
	{
		return CDXLPhysicalRandomMotion::Cast(dxl_motion)
			->IsDuplicateSensitive();
	}

	// other motion operators are not sensitive to duplicates
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::HasProjElem
//
//	@doc:
//		Check whether the given project list has a project element of the given
//		operator type
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::HasProjElem(CDXLNode *project_list_dxlnode, Edxlopid dxl_opid)
{
	GPOS_ASSERT(NULL != project_list_dxlnode);
	GPOS_ASSERT(EdxlopScalarProjectList ==
				project_list_dxlnode->GetOperator()->GetDXLOperator());
	GPOS_ASSERT(EdxlopSentinel > dxl_opid);

	const ULONG arity = project_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *dxl_project_element = (*project_list_dxlnode)[ul];
		GPOS_ASSERT(EdxlopScalarProjectElem ==
					dxl_project_element->GetOperator()->GetDXLOperator());

		CDXLNode *dxl_child_node = (*dxl_project_element)[0];
		if (dxl_opid == dxl_child_node->GetOperator()->GetDXLOperator())
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateDXLProjElemConstNULL
//
//	@doc:
//		Create a DXL project element node with a Const NULL of type provided
//		by the column descriptor.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::CreateDXLProjElemConstNULL(CMemoryPool *mp,
											 CMDAccessor *md_accessor,
											 CIdGenerator *pidgtorCol,
											 const IMDColumn *md_col)
{
	GPOS_ASSERT(NULL != md_col);
	GPOS_ASSERT(!md_col->IsSystemColumn());

	const WCHAR *col_name = md_col->Mdname().GetMDName()->GetBuffer();
	ULONG colid = pidgtorCol->next_id();
	CDXLNode *dxl_project_element = CreateDXLProjElemConstNULL(
		mp, md_accessor, md_col->MdidType(), colid, col_name);

	return dxl_project_element;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateDXLProjElemConstNULL
//
//	@doc:
//		Create a DXL project element node with a Const NULL expression
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::CreateDXLProjElemConstNULL(CMemoryPool *mp,
											 CMDAccessor *md_accessor,
											 IMDId *mdid, ULONG colid,
											 const WCHAR *col_name)
{
	CHAR *column_name =
		CDXLUtils::CreateMultiByteCharStringFromWCString(mp, col_name);
	CDXLNode *dxl_project_element =
		CreateDXLProjElemConstNULL(mp, md_accessor, mdid, colid, column_name);

	GPOS_DELETE_ARRAY(column_name);

	return dxl_project_element;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CreateDXLProjElemConstNULL
//
//	@doc:
//		Create a DXL project element node with a Const NULL expression
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::CreateDXLProjElemConstNULL(CMemoryPool *mp,
											 CMDAccessor *md_accessor,
											 IMDId *mdid, ULONG colid,
											 CHAR *alias_name)
{
	// get the id and alias for the proj elem
	CMDName *alias_mdname = NULL;

	if (NULL == alias_name)
	{
		CWStringConst unnamed_col(GPOS_WSZ_LIT("?column?"));
		alias_mdname = GPOS_NEW(mp) CMDName(mp, &unnamed_col);
	}
	else
	{
		CWStringDynamic *alias =
			CDXLUtils::CreateDynamicStringFromCharArray(mp, alias_name);
		alias_mdname = GPOS_NEW(mp) CMDName(mp, alias);
		GPOS_DELETE(alias);
	}

	mdid->AddRef();
	CDXLDatum *datum_dxl = NULL;
	if (mdid->Equals(&CMDIdGPDB::m_mdid_int2))
	{
		datum_dxl = GPOS_NEW(mp)
			CDXLDatumInt2(mp, mdid, true /*fConstNull*/, 0 /*value*/);
	}
	else if (mdid->Equals(&CMDIdGPDB::m_mdid_int4))
	{
		datum_dxl = GPOS_NEW(mp)
			CDXLDatumInt4(mp, mdid, true /*fConstNull*/, 0 /*value*/);
	}
	else if (mdid->Equals(&CMDIdGPDB::m_mdid_int8))
	{
		datum_dxl = GPOS_NEW(mp)
			CDXLDatumInt8(mp, mdid, true /*fConstNull*/, 0 /*value*/);
	}
	else if (mdid->Equals(&CMDIdGPDB::m_mdid_bool))
	{
		datum_dxl = GPOS_NEW(mp)
			CDXLDatumBool(mp, mdid, true /*fConstNull*/, 0 /*value*/);
	}
	else if (mdid->Equals(&CMDIdGPDB::m_mdid_oid))
	{
		datum_dxl = GPOS_NEW(mp)
			CDXLDatumOid(mp, mdid, true /*fConstNull*/, 0 /*value*/);
	}
	else
	{
		const IMDType *md_type = md_accessor->RetrieveType(mdid);
		datum_dxl = CMDTypeGenericGPDB::CreateDXLDatumVal(
			mp, mdid, md_type, default_type_modifier, true /*fConstNull*/,
			NULL,						   /*pba */
			0 /*length*/, 0 /*l_value*/, 0 /*dValue*/
		);
	}

	CDXLNode *dxl_const_node = GPOS_NEW(mp)
		CDXLNode(mp, GPOS_NEW(mp) CDXLScalarConstValue(mp, datum_dxl));

	return GPOS_NEW(mp)
		CDXLNode(mp, GPOS_NEW(mp) CDXLScalarProjElem(mp, colid, alias_mdname),
				 dxl_const_node);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CheckRTEPermissions
//
//	@doc:
//		Check permissions on range table
//
//---------------------------------------------------------------------------
void
CTranslatorUtils::CheckRTEPermissions(List *range_table_list)
{
	gpdb::CheckRTPermissions(range_table_list);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::UpdateGrpColMapping
//
//	@doc:
//		Update grouping columns permission mappings
//
//---------------------------------------------------------------------------
void
CTranslatorUtils::UpdateGrpColMapping(CMemoryPool *mp,
									  UlongToUlongMap *group_col_pos,
									  CBitSet *group_cols, ULONG sort_group_ref)
{
	GPOS_ASSERT(NULL != group_col_pos);
	GPOS_ASSERT(NULL != group_cols);

	if (!group_cols->Get(sort_group_ref))
	{
		ULONG num_unique_grouping_cols = group_cols->Size();
		group_col_pos->Insert(GPOS_NEW(mp) ULONG(num_unique_grouping_cols),
							  GPOS_NEW(mp) ULONG(sort_group_ref));
		(void) group_cols->ExchangeSet(sort_group_ref);
	}
}


//---------------------------------------------------------------------------
//      @function:
//              CTranslatorUtils::MarkOuterRefs
//
//      @doc:
//		check if given column ids are outer refs in the tree rooted by
//		given node
//---------------------------------------------------------------------------
void
CTranslatorUtils::MarkOuterRefs(
	ULONG *colids,	// array of column ids to be checked
	BOOL *
		is_outer_ref,  // array of outer ref indicators, initially all set to true by caller
	ULONG num_columns,	// number of columns
	CDXLNode *dxlnode)
{
	GPOS_ASSERT(NULL != colids);
	GPOS_ASSERT(NULL != is_outer_ref);
	GPOS_ASSERT(NULL != dxlnode);

	const CDXLOperator *dxl_op = dxlnode->GetOperator();
	for (ULONG ulCol = 0; ulCol < num_columns; ulCol++)
	{
		ULONG colid = colids[ulCol];
		if (is_outer_ref[ulCol] && dxl_op->IsColDefined(colid))
		{
			// column is defined by operator, reset outer reference flag
			is_outer_ref[ulCol] = false;
		}
	}

	// recursively process children
	const ULONG arity = dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		MarkOuterRefs(colids, is_outer_ref, num_columns, (*dxlnode)[ul]);
	}
}

//---------------------------------------------------------------------------
//      @function:
//              CTranslatorUtils::MapDXLSubplanToSublinkType
//
//      @doc:
//              Map DXL Subplan type to GPDB SubLinkType
//
//---------------------------------------------------------------------------
SubLinkType
CTranslatorUtils::MapDXLSubplanToSublinkType(EdxlSubPlanType dxl_subplan_type)
{
	GPOS_ASSERT(EdxlSubPlanTypeSentinel > dxl_subplan_type);
	ULONG mapping[][2] = {{EdxlSubPlanTypeScalar, EXPR_SUBLINK},
						  {EdxlSubPlanTypeExists, EXISTS_SUBLINK},
						  {EdxlSubPlanTypeNotExists, NOT_EXISTS_SUBLINK},
						  {EdxlSubPlanTypeAny, ANY_SUBLINK},
						  {EdxlSubPlanTypeAll, ALL_SUBLINK}};

	const ULONG arity = GPOS_ARRAY_SIZE(mapping);
	SubLinkType slink = EXPR_SUBLINK;
	BOOL found GPOS_ASSERTS_ONLY = false;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ULONG *elem = mapping[ul];
		if ((ULONG) dxl_subplan_type == elem[0])
		{
			slink = (SubLinkType) elem[1];
			found = true;
			break;
		}
	}

	GPOS_ASSERT(found && "Invalid SubPlanType");

	return slink;
}


//---------------------------------------------------------------------------
//      @function:
//              CTranslatorUtils::MapSublinkTypeToDXLSubplan
//
//      @doc:
//              Map GPDB SubLinkType to DXL subplan type
//
//---------------------------------------------------------------------------
EdxlSubPlanType
CTranslatorUtils::MapSublinkTypeToDXLSubplan(SubLinkType slink)
{
	ULONG mapping[][2] = {{EXPR_SUBLINK, EdxlSubPlanTypeScalar},
						  {EXISTS_SUBLINK, EdxlSubPlanTypeExists},
						  {NOT_EXISTS_SUBLINK, EdxlSubPlanTypeNotExists},
						  {ANY_SUBLINK, EdxlSubPlanTypeAny},
						  {ALL_SUBLINK, EdxlSubPlanTypeAll}};

	const ULONG arity = GPOS_ARRAY_SIZE(mapping);
	EdxlSubPlanType dxl_subplan_type = EdxlSubPlanTypeScalar;
	BOOL found GPOS_ASSERTS_ONLY = false;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ULONG *elem = mapping[ul];
		if ((ULONG) slink == elem[0])
		{
			dxl_subplan_type = (EdxlSubPlanType) elem[1];
			found = true;
			break;
		}
	}

	GPOS_ASSERT(found && "Invalid SubLinkType");

	return dxl_subplan_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::RelHasConstraints
//
//	@doc:
//		Check whether there are constraints for the given relation
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::RelHasConstraints(const IMDRelation *rel)
{
	if (0 < rel->CheckConstraintCount())
	{
		return true;
	}

	const ULONG num_cols = rel->ColumnCount();

	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		const IMDColumn *md_col = rel->GetMdCol(ul);
		if (!md_col->IsSystemColumn() && !md_col->IsNullable())
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetAssertErrorMsgs
//
//	@doc:
//		Construct a list of error messages from a list of assert constraints
//
//---------------------------------------------------------------------------
List *
CTranslatorUtils::GetAssertErrorMsgs(CDXLNode *assert_constraint_list)
{
	GPOS_ASSERT(NULL != assert_constraint_list);
	GPOS_ASSERT(EdxlopScalarAssertConstraintList ==
				assert_constraint_list->GetOperator()->GetDXLOperator());

	List *error_msgs_list = NIL;
	const ULONG num_constraints = assert_constraint_list->Arity();

	for (ULONG ul = 0; ul < num_constraints; ul++)
	{
		CDXLNode *dxl_constraint_node = (*assert_constraint_list)[ul];
		CDXLScalarAssertConstraint *dxl_constraint_op =
			CDXLScalarAssertConstraint::Cast(
				dxl_constraint_node->GetOperator());
		CWStringBase *error_msg = dxl_constraint_op->GetErrorMsgStr();
		error_msgs_list = gpdb::LAppend(
			error_msgs_list,
			gpdb::MakeStringValue(
				CreateMultiByteCharStringFromWCString(error_msg->GetBuffer())));
	}

	return error_msgs_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::GetNumNonSystemColumns
//
//	@doc:
//		Return the count of non-system columns in the relation
//
//---------------------------------------------------------------------------
ULONG
CTranslatorUtils::GetNumNonSystemColumns(const IMDRelation *rel)
{
	GPOS_ASSERT(NULL != rel);

	ULONG num_non_system_cols = 0;

	const ULONG num_cols = rel->ColumnCount();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		const IMDColumn *md_col = rel->GetMdCol(ul);

		if (!md_col->IsSystemColumn())
		{
			num_non_system_cols++;
		}
	}

	return num_non_system_cols;
}

// EOF
