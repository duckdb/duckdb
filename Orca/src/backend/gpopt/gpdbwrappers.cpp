//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		gpdbwrappers.cpp
//
//	@doc:
//		Implementation of GPDB function wrappers. Note that we should never
// 		return directly from inside the PG_TRY() block, in order to restore
//		the long jump stack. That is why we save the return value of the GPDB
//		function to a local variable and return it after the PG_END_TRY().
//		./README file contains the sources (caches and catalog tables) of metadata
//		requested by the optimizer and retrieved using GPDB function wrappers. Any
//		change to optimizer's requested metadata should also be recorded in ./README file.
//
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/gpdbwrappers.h"

#include "gpos/base.h"
#include "gpos/error/CAutoExceptionStack.h"
#include "gpos/error/CException.h"

#include "gpopt/utils/gpdbdefs.h"
#include "naucrates/exception.h"

#include "catalog/pg_collation.h"
extern "C" {
#include "access/external.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "parser/parse_agg.h"
#include "partitioning/partdesc.h"
#include "storage/lmgr.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
}
#define GP_WRAP_START                                            \
	sigjmp_buf local_sigjmp_buf;                                 \
	{                                                            \
		CAutoExceptionStack aes((void **) &PG_exception_stack,   \
								(void **) &error_context_stack); \
		if (0 == sigsetjmp(local_sigjmp_buf, 0))                 \
		{                                                        \
			aes.SetLocalJmp(&local_sigjmp_buf)

#define GP_WRAP_END                                        \
	}                                                      \
	else                                                   \
	{                                                      \
		GPOS_RAISE(gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError); \
	}                                                      \
	}

using namespace gpos;

bool
gpdb::BoolFromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetBool(d);
	}
	GP_WRAP_END;
	return false;
}

Datum
gpdb::DatumFromBool(bool b)
{
	GP_WRAP_START;
	{
		return BoolGetDatum(b);
	}
	GP_WRAP_END;
	return 0;
}

char
gpdb::CharFromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetChar(d);
	}
	GP_WRAP_END;
	return '\0';
}

Datum
gpdb::DatumFromChar(char c)
{
	GP_WRAP_START;
	{
		return CharGetDatum(c);
	}
	GP_WRAP_END;
	return 0;
}

int8
gpdb::Int8FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetInt8(d);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromInt8(int8 i8)
{
	GP_WRAP_START;
	{
		return Int8GetDatum(i8);
	}
	GP_WRAP_END;
	return 0;
}

uint8
gpdb::Uint8FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt8(d);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromUint8(uint8 ui8)
{
	GP_WRAP_START;
	{
		return UInt8GetDatum(ui8);
	}
	GP_WRAP_END;
	return 0;
}

int16
gpdb::Int16FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetInt16(d);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromInt16(int16 i16)
{
	GP_WRAP_START;
	{
		return Int16GetDatum(i16);
	}
	GP_WRAP_END;
	return 0;
}

uint16
gpdb::Uint16FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt16(d);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromUint16(uint16 ui16)
{
	GP_WRAP_START;
	{
		return UInt16GetDatum(ui16);
	}
	GP_WRAP_END;
	return 0;
}

int32
gpdb::Int32FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetInt32(d);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromInt32(int32 i32)
{
	GP_WRAP_START;
	{
		return Int32GetDatum(i32);
	}
	GP_WRAP_END;
	return 0;
}

uint32
gpdb::lUint32FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt32(d);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromUint32(uint32 ui32)
{
	GP_WRAP_START;
	{
		return UInt32GetDatum(ui32);
	}
	GP_WRAP_END;
	return 0;
}

int64
gpdb::Int64FromDatum(Datum d)
{
	Datum d2 = d;
	GP_WRAP_START;
	{
		return DatumGetInt64(d2);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromInt64(int64 i64)
{
	int64 ii64 = i64;
	GP_WRAP_START;
	{
		return Int64GetDatum(ii64);
	}
	GP_WRAP_END;
	return 0;
}

uint64
gpdb::Uint64FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt64(d);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromUint64(uint64 ui64)
{
	GP_WRAP_START;
	{
		return UInt64GetDatum(ui64);
	}
	GP_WRAP_END;
	return 0;
}

Oid
gpdb::OidFromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetObjectId(d);
	}
	GP_WRAP_END;
	return 0;
}

void *
gpdb::PointerFromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetPointer(d);
	}
	GP_WRAP_END;
	return NULL;
}

float4
gpdb::Float4FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetFloat4(d);
	}
	GP_WRAP_END;
	return 0;
}

float8
gpdb::Float8FromDatum(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetFloat8(d);
	}
	GP_WRAP_END;
	return 0;
}

Datum
gpdb::DatumFromPointer(const void *p)
{
	GP_WRAP_START;
	{
		return PointerGetDatum(p);
	}
	GP_WRAP_END;
	return 0;
}

bool
gpdb::AggregateExists(Oid oid)
{
	GP_WRAP_START;
	{
		return aggregate_exists(oid);
	}
	GP_WRAP_END;
	return false;
}

Bitmapset *
gpdb::BmsAddMember(Bitmapset *a, int x)
{
	GP_WRAP_START;
	{
		return bms_add_member(a, x);
	}
	GP_WRAP_END;
	return NULL;
}

void *
gpdb::CopyObject(void *from)
{
	GP_WRAP_START;
	{
		return copyObjectImpl(from);
	}
	GP_WRAP_END;
	return NULL;
}

Size
gpdb::DatumSize(Datum value, bool type_by_val, int iTypLen)
{
	GP_WRAP_START;
	{
		return datumGetSize(value, type_by_val, iTypLen);
	}
	GP_WRAP_END;
	return 0;
}

Node *
gpdb::MutateExpressionTree(Node *node, Node *(*mutator)(), void *context)
{
	GP_WRAP_START;
	{
		return expression_tree_mutator(node, mutator, context);
	}
	GP_WRAP_END;
	return NULL;
}

bool
gpdb::WalkExpressionTree(Node *node, bool (*walker)(), void *context)
{
	GP_WRAP_START;
	{
		return expression_tree_walker(node, walker, context);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::ExprType(Node *expr)
{
	GP_WRAP_START;
	{
		return exprType(expr);
	}
	GP_WRAP_END;
	return 0;
}

int32
gpdb::ExprTypeMod(Node *expr)
{
	GP_WRAP_START;
	{
		return exprTypmod(expr);
	}
	GP_WRAP_END;
	return 0;
}

Oid
gpdb::ExprCollation(Node *expr)
{
	GP_WRAP_START;
	{
		if (expr && IsA(expr, List))
		{
			// GPDB_91_MERGE_FIXME: collation
			List *exprlist = (List *) expr;
			ListCell *lc;

			Oid collation = InvalidOid;
			foreach (lc, exprlist)
			{
				Node *expr = (Node *) lfirst(lc);
				if ((collation = exprCollation(expr)) != InvalidOid)
				{
					break;
				}
			}
			return collation;
		}
		else
		{
			return exprCollation(expr);
		}
	}
	GP_WRAP_END;
	return 0;
}

Oid
gpdb::TypeCollation(Oid type)
{
	GP_WRAP_START;
	{
		Oid collation = InvalidOid;
		Oid typcollation = get_typcollation(type);
		// GPDB_12_MERGE_FIXME: brittle assumption: we only let in NAME,
		// default-collated non-name, or non-collatable expressions
		// This and a lot of other hacks can go away if only collation on
		// expressions just roundtrips through ORCA
		if (OidIsValid(typcollation))
		{
			if (type == NAMEOID)
				return typcollation;  // As of v12, this is C_COLLATION_OID
			return DEFAULT_COLLATION_OID;
		}
		return collation;
	}
	GP_WRAP_END;
	return 0;
}


List *
gpdb::ExtractNodesPlan(Plan *pl, int node_tag, bool descend_into_subqueries)
{
	GP_WRAP_START;
	{
		return extract_nodes_plan(pl, node_tag, descend_into_subqueries);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::ExtractNodesExpression(Node *node, int node_tag,
							 bool descend_into_subqueries)
{
	GP_WRAP_START;
	{
		return extract_nodes_expression(node, node_tag,
										descend_into_subqueries);
	}
	GP_WRAP_END;
	return NIL;
}

void
gpdb::FreeAttrStatsSlot(AttStatsSlot *sslot)
{
	GP_WRAP_START;
	{
		free_attstatsslot(sslot);
		return;
	}
	GP_WRAP_END;
}

bool
gpdb::IsFuncAllowedForPartitionSelection(Oid funcid)
{
	GP_WRAP_START;
	switch (funcid)
	{
			// These are the functions we have allowed as lossy casts for Partition selection.
			// For range partition selection, the logic in ORCA checks on bounds of the partition ranges.
			// Hence these must be increasing functions.
		case F_TIMESTAMP_DATE:	// date(timestamp) -> date
		case F_DTOI4:			// int4(float8) -> int4
		case F_FTOI4:			// int4(float4) -> int4
		case F_INT82:			// int2(int8) -> int2
		case F_INT84:			// int4(int8) -> int4
		case F_I4TOI2:			// int2(int4) -> int2
		case F_FTOI8:			// int8(float4) -> int8
		case F_FTOI2:			// int2(float4) -> int2
		case F_FLOAT4_NUMERIC:	// numeric(float4) -> numeric
		case F_DTOI8:			// int8(float8) -> int8
		case F_DTOI2:			// int2(float4) -> int2
		case F_DTOF:			// float4(float8) -> float4
		case F_FLOAT8_NUMERIC:	// numeric(float8) -> numeric
		case F_NUMERIC_INT8:	// int8(numeric) -> int8
		case F_NUMERIC_INT2:	// int2(numeric) -> int2
		case F_NUMERIC_INT4:	// int4(numeric) -> int4
			return true;
		default:
			return false;
	}
	GP_WRAP_END;
}

bool
gpdb::FuncStrict(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return func_strict(funcid);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::IsFuncNDVPreserving(Oid funcid)
{
	// Given a function oid, return whether it's one of a list of NDV-preserving
	// functions (estimated NDV of output is similar to that of the input)
	switch (funcid)
	{
		// for now, these are the functions we consider for this optimization
		case F_LOWER:
		case F_LTRIM1:
		case F_BTRIM1:
		case F_RTRIM1:
		case F_UPPER:
			return true;
		default:
			return false;
	}
}

char
gpdb::FuncStability(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return func_volatile(funcid);
	}
	GP_WRAP_END;
	return '\0';
}

char
gpdb::FuncDataAccess(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return func_data_access(funcid);
	}
	GP_WRAP_END;
	return '\0';
}

char
gpdb::FuncExecLocation(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return func_exec_location(funcid);
	}
	GP_WRAP_END;
	return '\0';
}

bool
gpdb::FunctionExists(Oid oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return function_exists(oid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetAggIntermediateResultType(Oid aggid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_aggregate */
		return get_agg_transtype(aggid);
	}
	GP_WRAP_END;
	return 0;
}

int
gpdb::GetAggregateArgTypes(Aggref *aggref, Oid *inputTypes)
{
	GP_WRAP_START;
	{
		return get_aggregate_argtypes(aggref, inputTypes);
	}
	GP_WRAP_END;
	return 0;
}

Oid
gpdb::ResolveAggregateTransType(Oid aggfnoid, Oid aggtranstype, Oid *inputTypes,
								int numArguments)
{
	GP_WRAP_START;
	{
		return resolve_aggregate_transtype(aggfnoid, aggtranstype, inputTypes,
										   numArguments);
	}
	GP_WRAP_END;
	return 0;
}

Query *
gpdb::FlattenJoinAliasVar(Query *query, gpos::ULONG query_level)
{
	GP_WRAP_START;
	{
		return flatten_join_alias_var_optimizer(query, query_level);
	}
	GP_WRAP_END;

	return NULL;
}

bool
gpdb::IsOrderedAgg(Oid aggid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_aggregate */
		return is_agg_ordered(aggid);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::IsAggPartialCapable(Oid aggid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_aggregate */
		return is_agg_partial_capable(aggid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetAggregate(const char *agg, Oid type_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_aggregate */
		return get_aggregate(agg, type_oid);
	}
	GP_WRAP_END;
	return 0;
}

Oid
gpdb::GetArrayType(Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type */
		return get_array_type(typid);
	}
	GP_WRAP_END;
	return 0;
}

bool
gpdb::GetAttrStatsSlot(AttStatsSlot *sslot, HeapTuple statstuple, int reqkind,
					   Oid reqop, int flags)
{
	GP_WRAP_START;
	{
		return get_attstatsslot(sslot, statstuple, reqkind, reqop, flags);
	}
	GP_WRAP_END;
	return false;
}

HeapTuple
gpdb::GetAttStats(Oid relid, AttrNumber attnum)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_statistic */
		return get_att_stats(relid, attnum);
	}
	GP_WRAP_END;
	return NULL;
}

Oid
gpdb::GetCommutatorOp(Oid opno)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator */
		return get_commutator(opno);
	}
	GP_WRAP_END;
	return 0;
}

bool
gpdb::CheckConstraintExists(Oid check_constraint_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_constraint */
		return check_constraint_exists(check_constraint_oid);
	}
	GP_WRAP_END;
	return false;
}

char *
gpdb::GetCheckConstraintName(Oid check_constraint_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_constraint */
		return get_check_constraint_name(check_constraint_oid);
	}
	GP_WRAP_END;
	return NULL;
}

Oid
gpdb::GetCheckConstraintRelid(Oid check_constraint_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_constraint */
		return get_check_constraint_relid(check_constraint_oid);
	}
	GP_WRAP_END;
	return 0;
}

Node *
gpdb::PnodeCheckConstraint(Oid check_constraint_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_constraint */
		return get_check_constraint_expr_tree(check_constraint_oid);
	}
	GP_WRAP_END;
	return NULL;
}

List *
gpdb::GetCheckConstraintOids(Oid rel_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_constraint */
		return get_check_constraint_oids(rel_oid);
	}
	GP_WRAP_END;
	return NULL;
}

Node *
gpdb::GetRelationPartConstraints(Relation rel)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition, pg_partition_rule, pg_constraint */
		List *part_quals = RelationGetPartitionQual(rel);
		if (part_quals)
		{
			return (Node *) make_ands_explicit(part_quals);
		}
	}
	GP_WRAP_END;
	return NULL;
}

#if 0
bool
gpdb::HasExternalPartition
	(
	Oid oid
	)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition, pg_partition_rule */
		return rel_has_external_partition(oid);
	}
	GP_WRAP_END;
	return false;
}


bool
gpdb::IsLeafPartition
	(
	Oid oid
	)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition, pg_partition_rule */
		return rel_is_leaf_partition(oid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetRootPartition
	(
	Oid oid
	)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition, pg_partition_rule */
		return rel_partition_get_master(oid);
	}
	GP_WRAP_END;
	return InvalidOid;
}
#endif

bool
gpdb::GetCastFunc(Oid src_oid, Oid dest_oid, bool *is_binary_coercible,
				  Oid *cast_fn_oid, CoercionPathType *pathtype)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_cast */
		return get_cast_func(src_oid, dest_oid, is_binary_coercible,
							 cast_fn_oid, pathtype);
	}
	GP_WRAP_END;
	return false;
}

unsigned int
gpdb::GetComparisonType(Oid op_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_amop */
		return get_comparison_type(op_oid);
	}
	GP_WRAP_END;
	return CmptOther;
}

Oid
gpdb::GetComparisonOperator(Oid left_oid, Oid right_oid, unsigned int cmpt)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_amop */
		return get_comparison_operator(left_oid, right_oid, (CmpType) cmpt);
	}
	GP_WRAP_END;
	return InvalidOid;
}

Oid
gpdb::GetEqualityOp(Oid type_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type */
		Oid eq_opr;

		get_sort_group_operators(type_oid, false, true, false, NULL, &eq_opr,
								 NULL, NULL);

		return eq_opr;
	}
	GP_WRAP_END;
	return InvalidOid;
}

Oid
gpdb::GetEqualityOpForOrderingOp(Oid opno, bool *reverse)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_amop */
		return get_equality_op_for_ordering_op(opno, reverse);
	}
	GP_WRAP_END;
	return InvalidOid;
}

Oid
gpdb::GetOrderingOpForEqualityOp(Oid opno, bool *reverse)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_amop */
		return get_ordering_op_for_equality_op(opno, reverse);
	}
	GP_WRAP_END;
	return InvalidOid;
}

char *
gpdb::GetFuncName(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return get_func_name(funcid);
	}
	GP_WRAP_END;
	return NULL;
}

List *
gpdb::GetFuncOutputArgTypes(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return get_func_output_arg_types(funcid);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::GetFuncArgTypes(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return get_func_arg_types(funcid);
	}
	GP_WRAP_END;
	return NIL;
}

bool
gpdb::GetFuncRetset(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return get_func_retset(funcid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetFuncRetType(Oid funcid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return get_func_rettype(funcid);
	}
	GP_WRAP_END;
	return 0;
}

Oid
gpdb::GetInverseOp(Oid opno)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator */
		return get_negator(opno);
	}
	GP_WRAP_END;
	return 0;
}

RegProcedure
gpdb::GetOpFunc(Oid opno)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator */
		return get_opcode(opno);
	}
	GP_WRAP_END;
	return 0;
}

char *
gpdb::GetOpName(Oid opno)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator */
		return get_opname(opno);
	}
	GP_WRAP_END;
	return NULL;
}
#if 0
List *
gpdb::GetPartitionAttrs
	(
	Oid oid
	)
{
	GP_WRAP_START;
	{
		// return unique partition level attributes
		/* catalog tables: pg_partition */
		return rel_partition_keys_ordered(oid);
	}
	GP_WRAP_END;
	return NIL;
}

void
gpdb::GetOrderedPartKeysAndKinds
	(
	Oid oid,
	List **pkeys,
	List **pkinds
	)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition */
		rel_partition_keys_kinds_ordered(oid, pkeys, pkinds);
	}
	GP_WRAP_END;
}

PartitionNode *
gpdb::GetParts
	(
	Oid relid,
	int16 level,
	Oid parent,
	bool inctemplate,
	bool includesubparts
	)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition, pg_partition_rule */
		return get_parts(relid, level, parent, inctemplate, includesubparts);
	}
	GP_WRAP_END;
	return NULL;
}
#endif

List *
gpdb::GetRelationKeys(Oid relid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_constraint */
		return get_relation_keys(relid);
	}
	GP_WRAP_END;
	return NIL;
}

Oid
gpdb::GetTypeRelid(Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type */
		return get_typ_typrelid(typid);
	}
	GP_WRAP_END;
	return 0;
}

char *
gpdb::GetTypeName(Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type */
		return get_type_name(typid);
	}
	GP_WRAP_END;
	return NULL;
}

int
gpdb::GetGPSegmentCount(void)
{
	GP_WRAP_START;
	{
		return getgpsegmentCount();
	}
	GP_WRAP_END;
	return 0;
}

bool
gpdb::HeapAttIsNull(HeapTuple tup, int attno)
{
	GP_WRAP_START;
	{
		return heap_attisnull(tup, attno, NULL);
	}
	GP_WRAP_END;
	return false;
}

void
gpdb::FreeHeapTuple(HeapTuple htup)
{
	GP_WRAP_START;
	{
		heap_freetuple(htup);
		return;
	}
	GP_WRAP_END;
}

bool
gpdb::IndexExists(Oid oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_index */
		return index_exists(oid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetDefaultDistributionOpclassForType(Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type, pg_opclass */
		return cdb_default_distribution_opclass_for_type(typid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetColumnDefOpclassForType(List *opclassName, Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type, pg_opclass */
		return cdb_get_opclass_for_column_def(opclassName, typid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetDefaultDistributionOpfamilyForType(Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type, pg_opclass */
		return cdb_default_distribution_opfamily_for_type(typid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetHashProcInOpfamily(Oid opfamily, Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_amproc, pg_type, pg_opclass */
		return cdb_hashproc_in_opfamily(opfamily, typid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::IsLegacyCdbHashFunction(Oid funcid)
{
	GP_WRAP_START;
	{
		return isLegacyCdbHashFunction(funcid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetLegacyCdbHashOpclassForBaseType(Oid typid)
{
	GP_WRAP_START;
	{
		return get_legacy_cdbhash_opclass_for_base_type(typid);
	}
	GP_WRAP_END;
	return false;
}

Oid
gpdb::GetOpclassFamily(Oid opclass)
{
	GP_WRAP_START;
	{
		return get_opclass_family(opclass);
	}
	GP_WRAP_END;
	return false;
}

List *
gpdb::LAppend(List *list, void *datum)
{
	GP_WRAP_START;
	{
		return lappend(list, datum);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::LAppendInt(List *list, int iDatum)
{
	GP_WRAP_START;
	{
		return lappend_int(list, iDatum);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::LAppendOid(List *list, Oid datum)
{
	GP_WRAP_START;
	{
		return lappend_oid(list, datum);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::LPrepend(void *datum, List *list)
{
	GP_WRAP_START;
	{
		return lcons(datum, list);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::LPrependInt(int datum, List *list)
{
	GP_WRAP_START;
	{
		return lcons_int(datum, list);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::LPrependOid(Oid datum, List *list)
{
	GP_WRAP_START;
	{
		return lcons_oid(datum, list);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::ListConcat(List *list1, List *list2)
{
	GP_WRAP_START;
	{
		return list_concat(list1, list2);
	}
	GP_WRAP_END;
	return NIL;
}

List *
gpdb::ListCopy(List *list)
{
	GP_WRAP_START;
	{
		return list_copy(list);
	}
	GP_WRAP_END;
	return NIL;
}

ListCell *
gpdb::ListHead(List *l)
{
	GP_WRAP_START;
	{
		return list_head(l);
	}
	GP_WRAP_END;
	return NULL;
}

ListCell *
gpdb::ListTail(List *l)
{
	GP_WRAP_START;
	{
		return list_tail(l);
	}
	GP_WRAP_END;
	return NULL;
}

uint32
gpdb::ListLength(List *l)
{
	GP_WRAP_START;
	{
		return list_length(l);
	}
	GP_WRAP_END;
	return 0;
}

void *
gpdb::ListNth(List *list, int n)
{
	GP_WRAP_START;
	{
		return list_nth(list, n);
	}
	GP_WRAP_END;
	return NULL;
}

int
gpdb::ListNthInt(List *list, int n)
{
	GP_WRAP_START;
	{
		return list_nth_int(list, n);
	}
	GP_WRAP_END;
	return 0;
}

Oid
gpdb::ListNthOid(List *list, int n)
{
	GP_WRAP_START;
	{
		return list_nth_oid(list, n);
	}
	GP_WRAP_END;
	return 0;
}

bool
gpdb::ListMemberOid(List *list, Oid oid)
{
	GP_WRAP_START;
	{
		return list_member_oid(list, oid);
	}
	GP_WRAP_END;
	return false;
}

void
gpdb::ListFree(List *list)
{
	GP_WRAP_START;
	{
		list_free(list);
		return;
	}
	GP_WRAP_END;
}

void
gpdb::ListFreeDeep(List *list)
{
	GP_WRAP_START;
	{
		list_free_deep(list);
		return;
	}
	GP_WRAP_END;
}
#if 0
bool
gpdb::IsAppendOnlyPartitionTable
	(
	Oid root_oid
	)
{
	GP_WRAP_START;
	{
		return rel_has_appendonly_partition(root_oid);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::IsMultilevelPartitionUniform
	(
	Oid root_oid
	)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition, pg_partition_rule, pg_constraint */
		return rel_partitioning_is_uniform(root_oid);
	}
	GP_WRAP_END;
	return false;
}
#endif

TypeCacheEntry *
gpdb::LookupTypeCache(Oid type_id, int flags)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type, pg_operator, pg_opclass, pg_opfamily, pg_amop */
		return lookup_type_cache(type_id, flags);
	}
	GP_WRAP_END;
	return NULL;
}

Value *
gpdb::MakeStringValue(char *str)
{
	GP_WRAP_START;
	{
		return makeString(str);
	}
	GP_WRAP_END;
	return NULL;
}

Value *
gpdb::MakeIntegerValue(long i)
{
	GP_WRAP_START;
	{
		return makeInteger(i);
	}
	GP_WRAP_END;
	return NULL;
}

Node *
gpdb::MakeIntConst(int32 intValue)
{
	GP_WRAP_START;
	{
		return (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
								  Int32GetDatum(intValue), false, true);
	}
	GP_WRAP_END;
}

Node *
gpdb::MakeBoolConst(bool value, bool isnull)
{
	GP_WRAP_START;
	{
		return makeBoolConst(value, isnull);
	}
	GP_WRAP_END;
	return NULL;
}

Node *
gpdb::MakeNULLConst(Oid type_oid)
{
	GP_WRAP_START;
	{
		return (Node *) makeNullConst(type_oid, -1 /*consttypmod*/, InvalidOid);
	}
	GP_WRAP_END;
	return NULL;
}

Node *
gpdb::MakeSegmentFilterExpr(int segid)
{
	GP_WRAP_START;
	{
		return (Node *) makeSegmentFilterExpr(segid);
	}
	GP_WRAP_END;
}

TargetEntry *
gpdb::MakeTargetEntry(Expr *expr, AttrNumber resno, char *resname, bool resjunk)
{
	GP_WRAP_START;
	{
		return makeTargetEntry(expr, resno, resname, resjunk);
	}
	GP_WRAP_END;
	return NULL;
}

Var *
gpdb::MakeVar(Index varno, AttrNumber varattno, Oid vartype, int32 vartypmod,
			  Index varlevelsup)
{
	GP_WRAP_START;
	{
		// GPDB_91_MERGE_FIXME: collation
		Oid collation = TypeCollation(vartype);
		return makeVar(varno, varattno, vartype, vartypmod, collation,
					   varlevelsup);
	}
	GP_WRAP_END;
	return NULL;
}

void *
gpdb::MemCtxtAllocZeroAligned(MemoryContext context, Size size)
{
	GP_WRAP_START;
	{
		return MemoryContextAllocZeroAligned(context, size);
	}
	GP_WRAP_END;
	return NULL;
}

void *
gpdb::MemCtxtAllocZero(MemoryContext context, Size size)
{
	GP_WRAP_START;
	{
		return MemoryContextAllocZero(context, size);
	}
	GP_WRAP_END;
	return NULL;
}

void *
gpdb::MemCtxtRealloc(void *pointer, Size size)
{
	GP_WRAP_START;
	{
		return repalloc(pointer, size);
	}
	GP_WRAP_END;
	return NULL;
}

char *
gpdb::MemCtxtStrdup(MemoryContext context, const char *string)
{
	GP_WRAP_START;
	{
		return MemoryContextStrdup(context, string);
	}
	GP_WRAP_END;
	return NULL;
}

// Helper function to throw an error with errcode, message and hint, like you
// would with ereport(...) in the backend. This could be extended for other
// fields, but this is all we need at the moment.
void
gpdb::GpdbEreportImpl(int xerrcode, int severitylevel, const char *xerrmsg,
					  const char *xerrhint, const char *filename, int lineno,
					  const char *funcname)
{
	GP_WRAP_START;
	{
		// We cannot use the ereport() macro here, because we want to pass on
		// the caller's filename and line number. This is essentially an
		// expanded version of ereport(). It will be caught by the
		// GP_WRAP_END, and propagated up as a C++ exception, to be
		// re-thrown as a Postgres error once we leave the C++ land.
		if (errstart(severitylevel, TEXTDOMAIN))
		{
			errcode(xerrcode);
			errmsg("%s", xerrmsg);
			if (xerrhint)
				errhint("%s", xerrhint);
			errfinish(filename, lineno, funcname);
		}
	}
	GP_WRAP_END;
}

char *
gpdb::NodeToString(void *obj)
{
	GP_WRAP_START;
	{
		return nodeToString(obj);
	}
	GP_WRAP_END;
	return NULL;
}

Node *
gpdb::StringToNode(char *string)
{
	GP_WRAP_START;
	{
		return (Node *) stringToNode(string);
	}
	GP_WRAP_END;
	return NULL;
}


Node *
gpdb::GetTypeDefault(Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type */
		return get_typdefault(typid);
	}
	GP_WRAP_END;
	return NULL;
}


double
gpdb::NumericToDoubleNoOverflow(Numeric num)
{
	GP_WRAP_START;
	{
		return numeric_to_double_no_overflow(num);
	}
	GP_WRAP_END;
	return 0.0;
}

bool
gpdb::NumericIsNan(Numeric num)
{
	GP_WRAP_START;
	{
		return numeric_is_nan(num);
	}
	GP_WRAP_END;
	return false;
}

double
gpdb::ConvertTimeValueToScalar(Datum datum, Oid typid)
{
	bool failure = false;
	GP_WRAP_START;
	{
		return convert_timevalue_to_scalar(datum, typid, &failure);
	}
	GP_WRAP_END;
	return 0.0;
}

double
gpdb::ConvertNetworkToScalar(Datum datum, Oid typid)
{
	bool failure = false;
	GP_WRAP_START;
	{
		return convert_network_to_scalar(datum, typid, &failure);
	}
	GP_WRAP_END;
	return 0.0;
}

bool
gpdb::IsOpHashJoinable(Oid opno, Oid inputtype)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator */
		return op_hashjoinable(opno, inputtype);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::IsOpMergeJoinable(Oid opno, Oid inputtype)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator */
		return op_mergejoinable(opno, inputtype);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::IsOpStrict(Oid opno)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator, pg_proc */
		return op_strict(opno);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::IsOpNDVPreserving(Oid opno)
{
	switch (opno)
	{
		// for now, we consider only the concatenation op as NDV-preserving
		// (note that we do additional checks later, e.g. col || 'const' is
		// NDV-preserving, while col1 || col2 is not)
		case OIDTextConcatenateOperator:
			return true;
		default:
			return false;
	}
}

void
gpdb::GetOpInputTypes(Oid opno, Oid *lefttype, Oid *righttype)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator */
		op_input_types(opno, lefttype, righttype);
		return;
	}
	GP_WRAP_END;
}

bool
gpdb::OperatorExists(Oid oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_operator */
		return operator_exists(oid);
	}
	GP_WRAP_END;
	return false;
}

void *
gpdb::GPDBAlloc(Size size)
{
	GP_WRAP_START;
	{
		return palloc(size);
	}
	GP_WRAP_END;
	return NULL;
}

void
gpdb::GPDBFree(void *ptr)
{
	GP_WRAP_START;
	{
		pfree(ptr);
		return;
	}
	GP_WRAP_END;
}

bool
gpdb::WalkQueryOrExpressionTree(Node *node, bool (*walker)(), void *context,
								int flags)
{
	GP_WRAP_START;
	{
		return query_or_expression_tree_walker(node, walker, context, flags);
	}
	GP_WRAP_END;
	return false;
}

Node *
gpdb::MutateQueryOrExpressionTree(Node *node, Node *(*mutator)(), void *context,
								  int flags)
{
	GP_WRAP_START;
	{
		return query_or_expression_tree_mutator(node, mutator, context, flags);
	}
	GP_WRAP_END;
	return NULL;
}

Query *
gpdb::MutateQueryTree(Query *query, Node *(*mutator)(), void *context,
					  int flags)
{
	GP_WRAP_START;
	{
		return query_tree_mutator(query, mutator, context, flags);
	}
	GP_WRAP_END;
	return NULL;
}

#if 0
bool
gpdb::RelPartIsRoot
	(
	Oid relid
	)
{
	GP_WRAP_START;
	{
		return PART_STATUS_ROOT == rel_part_status(relid);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::RelPartIsInterior
	(
	Oid relid
	)
{
	GP_WRAP_START;
	{
		return PART_STATUS_INTERIOR == rel_part_status(relid);
	}
	GP_WRAP_END;
	return false;
}
#endif

bool
gpdb::RelPartIsNone(Oid relid)
{
	// FIXME: this is left unused for now. But when it's used, we also need to
	// check for whether relid is a partition of another table
	pg_unreachable();
	return !RelIsPartitioned(relid);
}

bool
gpdb::HasSubclassSlow(Oid rel_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_inherits */
		return has_subclass_slow(rel_oid);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::RelIsExternalTable(Oid relid)
{
	GP_WRAP_START;
	{
		return rel_is_external_table(relid);
	}
	GP_WRAP_END;
	return false;
}

GpPolicy *
gpdb::GetDistributionPolicy(Relation rel)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_class */
		return relation_policy(rel);
	}
	GP_WRAP_END;
	return NULL;
}

#if 0
gpos::BOOL
gpdb::IsChildPartDistributionMismatched
	(
	Relation rel
	)
{
    GP_WRAP_START;
    {
    	/* catalog tables: pg_class, pg_inherits */
    	return child_distribution_mismatch(rel);
    }
    GP_WRAP_END;
    return false;
}
#endif

bool
gpdb::RelationExists(Oid oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_class */
		return relation_exists(oid);
	}
	GP_WRAP_END;
	return false;
}

void
gpdb::CdbEstimateRelationSize(RelOptInfo *relOptInfo, Relation rel,
							  int32 *attr_widths, BlockNumber *pages,
							  double *tuples, double *allvisfrac)
{
	GP_WRAP_START;
	{
		cdb_estimate_rel_size(relOptInfo, rel, attr_widths, pages, tuples,
							  allvisfrac);
		return;
	}
	GP_WRAP_END;
}

double
gpdb::CdbEstimatePartitionedNumTuples(Relation rel)
{
	GP_WRAP_START;
	{
		return cdb_estimate_partitioned_numtuples(rel);
	}
	GP_WRAP_END;
}

void
gpdb::CloseRelation(Relation rel)
{
	GP_WRAP_START;
	{
		RelationClose(rel);
		return;
	}
	GP_WRAP_END;
}

List *
gpdb::GetRelationIndexes(Relation relation)
{
	GP_WRAP_START;
	{
		if (relation->rd_rel->relhasindex)
		{
			/* catalog tables: from relcache */
			return RelationGetIndexList(relation);
		}
	}
	GP_WRAP_END;
	return NIL;
}
#if 0
LogicalIndexes *
gpdb::GetLogicalPartIndexes
	(
	Oid oid
	)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition, pg_partition_rule, pg_index */
		return BuildLogicalIndexInfo(oid);
	}
	GP_WRAP_END;
	return NULL;
}
LogicalIndexInfo *
gpdb::GetLogicalIndexInfo
	(
	Oid root_oid,
	Oid index_oid
	)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_index */
		return logicalIndexInfoForIndexOid(root_oid, index_oid);
	}
	GP_WRAP_END;
	return NULL;
}
#endif

gpdb::RelationWrapper
gpdb::GetRelation(Oid rel_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: relcache */
		return RelationWrapper{RelationIdGetRelation(rel_oid)};
	}
	GP_WRAP_END;
}

ExtTableEntry *
gpdb::GetExternalTableEntry(Oid rel_oid)
{
	GP_WRAP_START;
	{
		return GetExtTableEntry(rel_oid);
	}
	GP_WRAP_END;
	return NULL;
}


ForeignScan *
gpdb::CreateForeignScanForExternalTable(Oid rel_oid, Index scanrelid,
										List *qual, List *targetlist)
{
	GP_WRAP_START;
	{
		return BuildForeignScanForExternalTable(rel_oid, scanrelid, qual,
												targetlist);
	}
	GP_WRAP_END;
	return NULL;
}

// GPDB_12_MERGE_FIXME: Change signature to take in Expr instead of Node
TargetEntry *
gpdb::FindFirstMatchingMemberInTargetList(Node *node, List *targetlist)
{
	GP_WRAP_START;
	{
		return tlist_member((Expr *) node, targetlist);
	}
	GP_WRAP_END;
	return NULL;
}

List *
gpdb::FindMatchingMembersInTargetList(Node *node, List *targetlist)
{
	GP_WRAP_START;
	{
		return tlist_members(node, targetlist);
	}
	GP_WRAP_END;

	return NIL;
}

bool
gpdb::Equals(void *p1, void *p2)
{
	GP_WRAP_START;
	{
		return equal(p1, p2);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::TypeExists(Oid oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type */
		return type_exists(oid);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::IsCompositeType(Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type */
		return type_is_rowtype(typid);
	}
	GP_WRAP_END;
	return false;
}

bool
gpdb::IsTextRelatedType(Oid typid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type */
		char typcategory;
		bool typispreferred;
		get_type_category_preferred(typid, &typcategory, &typispreferred);

		return typcategory == TYPCATEGORY_STRING;
	}
	GP_WRAP_END;
	return false;
}


int
gpdb::GetIntFromValue(Node *node)
{
	GP_WRAP_START;
	{
		return intVal(node);
	}
	GP_WRAP_END;
	return 0;
}

Uri *
gpdb::ParseExternalTableUri(const char *uri)
{
	GP_WRAP_START;
	{
		return ParseExternalTableUri(uri);
	}
	GP_WRAP_END;
	return NULL;
}

CdbComponentDatabases *
gpdb::GetComponentDatabases(void)
{
	GP_WRAP_START;
	{
		/* catalog tables: gp_segment_config */
		return cdbcomponent_getCdbComponents();
	}
	GP_WRAP_END;
	return NULL;
}

int
gpdb::StrCmpIgnoreCase(const char *s1, const char *s2)
{
	GP_WRAP_START;
	{
		return pg_strcasecmp(s1, s2);
	}
	GP_WRAP_END;
	return 0;
}

bool *
gpdb::ConstructRandomSegMap(int total_primaries, int total_to_skip)
{
	GP_WRAP_START;
	{
		return makeRandomSegMap(total_primaries, total_to_skip);
	}
	GP_WRAP_END;
	return NULL;
}

StringInfo
gpdb::MakeStringInfo(void)
{
	GP_WRAP_START;
	{
		return makeStringInfo();
	}
	GP_WRAP_END;
	return NULL;
}

void
gpdb::AppendStringInfo(StringInfo str, const char *str1, const char *str2)
{
	GP_WRAP_START;
	{
		appendStringInfo(str, "%s%s", str1, str2);
		return;
	}
	GP_WRAP_END;
}

int
gpdb::FindNodes(Node *node, List *nodeTags)
{
	GP_WRAP_START;
	{
		return find_nodes(node, nodeTags);
	}
	GP_WRAP_END;
	return -1;
}

int
gpdb::CheckCollation(Node *node)
{
	GP_WRAP_START;
	{
		return check_collation(node);
	}
	GP_WRAP_END;
	return -1;
}

Node *
gpdb::CoerceToCommonType(ParseState *pstate, Node *node, Oid target_type,
						 const char *context)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_type, pg_cast */
		return coerce_to_common_type(pstate, node, target_type, context);
	}
	GP_WRAP_END;
	return NULL;
}

bool
gpdb::ResolvePolymorphicArgType(int numargs, Oid *argtypes, char *argmodes,
								FuncExpr *call_expr)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_proc */
		return resolve_polymorphic_argtypes(numargs, argtypes, argmodes,
											(Node *) call_expr);
	}
	GP_WRAP_END;
	return false;
}

// hash a list of const values with GPDB's hash function
int32
gpdb::CdbHashConstList(List *constants, int num_segments, Oid *hashfuncs)
{
	GP_WRAP_START;
	{
		return cdbhash_const_list(constants, num_segments, hashfuncs);
	}
	GP_WRAP_END;
	return 0;
}

unsigned int
gpdb::CdbHashRandomSeg(int num_segments)
{
	GP_WRAP_START;
	{
		return cdbhashrandomseg(num_segments);
	}
	GP_WRAP_END;
	return 0;
}

// check permissions on range table
void
gpdb::CheckRTPermissions(List *rtable)
{
	GP_WRAP_START;
	{
		ExecCheckRTPerms(rtable, true);
		return;
	}
	GP_WRAP_END;
}


// check that a table doesn't have UPDATE triggers.
bool
gpdb::HasUpdateTriggers(Oid relid)
{
	GP_WRAP_START;
	{
		return has_update_triggers(relid);
	}
	GP_WRAP_END;
	return false;
}

// get index op family properties
void
gpdb::IndexOpProperties(Oid opno, Oid opfamily, int *strategy, Oid *subtype)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_amop */

		// Only the right type is returned to the caller, the left
		// type is simply ignored.
		Oid lefttype;

		get_op_opfamily_properties(opno, opfamily, false, strategy, &lefttype,
								   subtype);
		return;
	}
	GP_WRAP_END;
}

// get oids of opfamilies for the index keys
List *
gpdb::GetIndexOpFamilies(Oid index_oid)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_index */

		// We return the operator families of the index keys.
		return get_index_opfamilies(index_oid);
	}
	GP_WRAP_END;

	return NIL;
}

// get oids of families this operator belongs to
List *
gpdb::GetOpFamiliesForScOp(Oid opno)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_amop */

		// We return the operator families this operator
		// belongs to.
		return get_operator_opfamilies(opno);
	}
	GP_WRAP_END;

	return NIL;
}

// get the OID of hash equality operator(s) compatible with the given op
Oid
gpdb::GetCompatibleHashOpFamily(Oid opno)
{
	GP_WRAP_START;
	{
		return get_compatible_hash_opfamily(opno);
	}
	GP_WRAP_END;
	return InvalidOid;
}

// get the OID of hash equality operator(s) compatible with the given op
Oid
gpdb::GetCompatibleLegacyHashOpFamily(Oid opno)
{
	GP_WRAP_START;
	{
		return get_compatible_legacy_hash_opfamily(opno);
	}
	GP_WRAP_END;
	return InvalidOid;
}

List *
gpdb::GetMergeJoinOpFamilies(Oid opno)
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_amop */

		return get_mergejoin_opfamilies(opno);
	}
	GP_WRAP_END;
	return NIL;
}


// Evaluates 'expr' and returns the result as an Expr.
// Caller keeps ownership of 'expr' and takes ownership of the result
Expr *
gpdb::EvaluateExpr(Expr *expr, Oid result_type, int32 typmod)
{
	GP_WRAP_START;
	{
		// GPDB_91_MERGE_FIXME: collation
		return evaluate_expr(expr, result_type, typmod, InvalidOid);
	}
	GP_WRAP_END;
	return NULL;
}

// interpret the value of "With oids" option from a list of defelems
// GPDB_12_MERGE_FIXME: this leaves dead code in CMDRelationGPDB
bool
gpdb::InterpretOidsOption(List *options, bool allowOids)
{
	GP_WRAP_START;
	{
		return false;
	}
	GP_WRAP_END;
	return false;
}

char *
gpdb::DefGetString(DefElem *defelem)
{
	GP_WRAP_START;
	{
		return defGetString(defelem);
	}
	GP_WRAP_END;
	return NULL;
}

Expr *
gpdb::TransformArrayConstToArrayExpr(Const *c)
{
	GP_WRAP_START;
	{
		return transform_array_Const_to_ArrayExpr(c);
	}
	GP_WRAP_END;
	return NULL;
}

Node *
gpdb::EvalConstExpressions(Node *node)
{
	GP_WRAP_START;
	{
		return eval_const_expressions(NULL, node);
	}
	GP_WRAP_END;
	return NULL;
}
#if 0
SelectedParts *
gpdb::RunStaticPartitionSelection
	(
	PartitionSelector *ps
	)
{
	GP_WRAP_START;
	{
		return static_part_selection(ps);
	}
	GP_WRAP_END;
	return NULL;
}
#endif

#ifdef FAULT_INJECTOR
FaultInjectorType_e
gpdb::InjectFaultInOptTasks(const char *fault_name)
{
	GP_WRAP_START;
	{
		return FaultInjector_InjectFaultIfSet(fault_name, DDLNotSpecified, "",
											  "");
	}
	GP_WRAP_END;
	return FaultInjectorTypeNotSpecified;
}
#endif

/* GPDB_12_MERGE_FIXME: dead code now? */
#if 0
gpos::ULONG
gpdb::CountLeafPartTables
       (
       Oid rel_oid
       )
{
	GP_WRAP_START;
	{
		/* catalog tables: pg_partition, pg_partition_rules */
		return countLeafPartTables(rel_oid);
	}
	GP_WRAP_END;

	return 0;
}
#endif

/*
 * To detect changes to catalog tables that require resetting the Metadata
 * Cache, we use the normal PostgreSQL catalog cache invalidation mechanism.
 * We register a callback to a cache on all the catalog tables that contain
 * information that's contained in the ORCA metadata cache.

 * There is no fine-grained mechanism in the metadata cache for invalidating
 * individual entries ATM, so we just blow the whole cache whenever anything
 * changes. The callback simply increments a counter. Whenever we start
 * planning a query, we check the counter to see if it has changed since the
 * last planned query, and reset the whole cache if it has.
 *
 * To make sure we've covered all catalog tables that contain information
 * that's stored in the metadata cache, there are "catalog tables: xxx"
 * comments in all the calls to backend functions in this file. They indicate
 * which catalog tables each function uses. We conservatively assume that
 * anything fetched via the wrapper functions in this file can end up in the
 * metadata cache and hence need to have an invalidation callback registered.
 */
static bool mdcache_invalidation_counter_registered = false;
static int64 mdcache_invalidation_counter = 0;
static int64 last_mdcache_invalidation_counter = 0;

static void
mdsyscache_invalidation_counter_callback(Datum arg, int cacheid,
										 uint32 hashvalue)
{
	mdcache_invalidation_counter++;
}

static void
mdrelcache_invalidation_counter_callback(Datum arg, Oid relid)
{
	mdcache_invalidation_counter++;
}

static void
register_mdcache_invalidation_callbacks(void)
{
	/* These are all the catalog tables that we care about. */
	int metadata_caches[] = {
		AGGFNOID,		  /* pg_aggregate */
		AMOPOPID,		  /* pg_amop */
		CASTSOURCETARGET, /* pg_cast */
		CONSTROID,		  /* pg_constraint */
		OPEROID,		  /* pg_operator */
		OPFAMILYOID,	  /* pg_opfamily */
#if 0
		PARTOID,			/* pg_partition */
		PARTRULEOID,		/* pg_partition_rule */
#endif
		STATRELATTINH, /* pg_statistics */
		TYPEOID,	   /* pg_type */
		PROCOID,	   /* pg_proc */

		/*
		 * lookup_type_cache() will also access pg_opclass, via GetDefaultOpClass(),
		 * but there is no syscache for it. Postgres doesn't seem to worry about
		 * invalidating the type cache on updates to pg_opclass, so we don't
		 * worry about that either.
		 */
		/* pg_opclass */

		/*
		 * Information from the following catalogs are included in the
		 * relcache, and any updates will generate relcache invalidation
		 * event. We'll catch the relcache invalidation event and don't need
		 * to register a catcache callback for them.
		 */
		/* pg_class */
		/* pg_index */

		/*
		 * pg_foreign_table is updated when a new external table is dropped/created,
		 * which will trigger a relcache invalidation event.
		 */
		/* pg_foreign_table */

		/*
		 * XXX: no syscache on pg_inherits. Is that OK? For any partitioning
		 * changes, I think there will also be updates on pg_partition and/or
		 * pg_partition_rules.
		 */
		/* pg_inherits */

		/*
		 * We assume that gp_segment_config will not change on the fly in a way that
		 * would affect ORCA
		 */
		/* gp_segment_config */
	};
	unsigned int i;

	for (i = 0; i < lengthof(metadata_caches); i++)
	{
		CacheRegisterSyscacheCallback(metadata_caches[i],
									  &mdsyscache_invalidation_counter_callback,
									  (Datum) 0);
	}

	/* also register the relcache callback */
	CacheRegisterRelcacheCallback(&mdrelcache_invalidation_counter_callback,
								  (Datum) 0);
}

// Has there been any catalog changes since last call?
bool
gpdb::MDCacheNeedsReset(void)
{
	GP_WRAP_START;
	{
		if (!mdcache_invalidation_counter_registered)
		{
			register_mdcache_invalidation_callbacks();
			mdcache_invalidation_counter_registered = true;
		}
		if (last_mdcache_invalidation_counter == mdcache_invalidation_counter)
			return false;
		else
		{
			last_mdcache_invalidation_counter = mdcache_invalidation_counter;
			return true;
		}
	}
	GP_WRAP_END;

	return true;
}

// returns true if a query cancel is requested in GPDB
bool
gpdb::IsAbortRequested(void)
{
	// No GP_WRAP_START/END needed here. We just check these global flags,
	// it cannot throw an ereport().
	return (QueryCancelPending || ProcDiePending);
}

// Given the type OID, get the typelem (InvalidOid if not an array type).
Oid
gpdb::GetElementType(Oid array_type_oid)
{
	GP_WRAP_START;
	{
		return get_element_type(array_type_oid);
	}
	GP_WRAP_END;
}

GpPolicy *
gpdb::MakeGpPolicy(GpPolicyType ptype, int nattrs, int numsegments)
{
	GP_WRAP_START;
	{
		/*
		 * FIXME_TABLE_EXPAND: it used by ORCA, help...
		 */
		return makeGpPolicy(ptype, nattrs, numsegments);
	}
	GP_WRAP_END;
}

uint32
gpdb::HashChar(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt32(DirectFunctionCall1(hashchar, d));
	}
	GP_WRAP_END;
}

uint32
gpdb::HashBpChar(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt32(
			DirectFunctionCall1Coll(hashbpchar, C_COLLATION_OID, d));
	}
	GP_WRAP_END;
}

uint32
gpdb::HashText(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt32(
			DirectFunctionCall1Coll(hashtext, C_COLLATION_OID, d));
	}
	GP_WRAP_END;
}

uint32
gpdb::HashName(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt32(DirectFunctionCall1(hashname, d));
	}
	GP_WRAP_END;
}

uint32
gpdb::UUIDHash(Datum d)
{
	GP_WRAP_START;
	{
		return DatumGetUInt32(DirectFunctionCall1(uuid_hash, d));
	}
	GP_WRAP_END;
}

void *
gpdb::GPDBMemoryContextAlloc(MemoryContext context, Size size)
{
	GP_WRAP_START;
	{
		return MemoryContextAlloc(context, size);
	}
	GP_WRAP_END;
	return NULL;
}

void
gpdb::GPDBMemoryContextDelete(MemoryContext context)
{
	GP_WRAP_START;
	{
		MemoryContextDelete(context);
	}
	GP_WRAP_END;
}

MemoryContext
gpdb::GPDBAllocSetContextCreate()
{
	GP_WRAP_START;
	{
		MemoryContext cxt;

		cxt =
			AllocSetContextCreate(OptimizerMemoryContext, "GPORCA memory pool",
								  ALLOCSET_DEFAULT_SIZES);
		/*
		 * Declare it as accounting root so that we can call
		 * MemoryContextGetCurrentSpace() on it.
		 */
		MemoryContextDeclareAccountingRoot(cxt);

		return cxt;
	}
	GP_WRAP_END;
	return NULL;
}

bool
gpdb::ExpressionReturnsSet(Node *clause)
{
	GP_WRAP_START;
	{
		return expression_returns_set(clause);
	}
	GP_WRAP_END;
}

bool
gpdb::RelIsPartitioned(Oid relid)
{
	GP_WRAP_START;
	{
		return relation_is_partitioned(relid);
	}
	GP_WRAP_END;
}

void
gpdb::GPDBLockRelationOid(Oid reloid, LOCKMODE lockmode)
{
	GP_WRAP_START;
	{
		bool lockUpgraded;
		lockmode = UpgradeRelLockIfNecessary(reloid, lockmode, &lockUpgraded);
		LockRelationOid(reloid, lockmode);
	}
	GP_WRAP_END;
}

// EOF
