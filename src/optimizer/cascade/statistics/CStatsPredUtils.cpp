//---------------------------------------------------------------------------
//	@filename:
//		CStatsPredUtils.cpp
//
//	@doc:
//		Statistics predicate helper routines
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CStatsPredUtils.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCastUtils.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/operators/CExpressionUtils.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/base/IDatum.h"
#include "duckdb/optimizer/cascade/base/IDatumBool.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/md/IMDTypeBool.h"
#include "duckdb/optimizer/cascade/statistics/CHistogram.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredArrayCmp.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredConj.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredDisj.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredLike.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::StatsCmpType
//
//	@doc:
//		For the purpose of statistics computation, what is the comparison
//		type of an operator. Note that this has different, looser semantics
//		than CUtils::ParseCmpType
//
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::StatsCmpType(const CWStringConst *str_opname)
{
	GPOS_ASSERT(NULL != str_opname);

	CStatsPred::EStatsCmpType stats_cmp_type = CStatsPred::EstatscmptOther;

	CWStringConst str_eq(GPOS_WSZ_LIT("="));
	CWStringConst str_lt(GPOS_WSZ_LIT("<"));
	CWStringConst str_leq(GPOS_WSZ_LIT("<="));
	CWStringConst str_geq(GPOS_WSZ_LIT(">="));
	CWStringConst str_gt(GPOS_WSZ_LIT(">"));
	CWStringConst str_neq(GPOS_WSZ_LIT("<>"));

	if (str_opname->Equals(&str_eq))
	{
		stats_cmp_type = CStatsPred::EstatscmptEq;
	}
	else if (str_opname->Equals(&str_lt))
	{
		stats_cmp_type = CStatsPred::EstatscmptL;
	}
	else if (str_opname->Equals(&str_leq))
	{
		stats_cmp_type = CStatsPred::EstatscmptLEq;
	}
	else if (str_opname->Equals(&str_geq))
	{
		stats_cmp_type = CStatsPred::EstatscmptGEq;
	}
	else if (str_opname->Equals(&str_gt))
	{
		stats_cmp_type = CStatsPred::EstatscmptG;
	}
	else if (str_opname->Equals(&str_neq))
	{
		stats_cmp_type = CStatsPred::EstatscmptNEq;
	}

	return stats_cmp_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::StatsCmpType
//
//	@doc:
//		For the purpose of statistics computation, what is the comparison
//		type of an operator. Note that this has different, looser semantics
//		than CUtils::ParseCmpType
//
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::StatsCmpType(IMDId *mdid)
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDScalarOp *md_scalar_op = md_accessor->RetrieveScOp(mdid);

	// Simply go by operator name.
	// If the name of the operator is "<", then it is a LessThan etc.
	const CWStringConst *str_opname = md_scalar_op->Mdname().GetMDName();

	return StatsCmpType(str_opname);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::CreateStatsPredUnsupported
//
//	@doc:
//		Create an unsupported statistics predicate
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::CreateStatsPredUnsupported(CMemoryPool *mp,
											CExpression *,	// predicate_expr,
											CColRefSet *	//outer_refs
)
{
	return GPOS_NEW(mp)
		CStatsPredUnsupported(gpos::ulong_max, CStatsPred::EstatscmptOther);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetStatsPredNullTest
//
//	@doc:
//		Extract statistics filtering information from a null test
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetStatsPredNullTest(CMemoryPool *mp,
									  CExpression *predicate_expr,
									  CColRefSet *	//outer_refs
)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(IsPredScalarIdentIsNull(predicate_expr) ||
				IsPredScalarIdentIsNotNull(predicate_expr));

	CExpression *expr_null_test = predicate_expr;
	CStatsPred::EStatsCmpType stats_cmp_type =
		CStatsPred::EstatscmptEq;  // 'is null'

	if (IsPredScalarIdentIsNotNull(predicate_expr))
	{
		expr_null_test = (*predicate_expr)[0];
		stats_cmp_type = CStatsPred::EstatscmptNEq;	 // 'is not null'
	}

	CScalarIdent *scalar_ident_op =
		CScalarIdent::PopConvert((*expr_null_test)[0]->Pop());
	const CColRef *col_ref = scalar_ident_op->Pcr();

	IDatum *datum = CStatisticsUtils::DatumNull(col_ref);
	if (!datum->StatsAreComparable(datum))
	{
		// stats calculations on such datums unsupported
		datum->Release();

		return GPOS_NEW(mp)
			CStatsPredUnsupported(col_ref->Id(), stats_cmp_type);
	}

	CPoint *point = GPOS_NEW(mp) CPoint(datum);
	CStatsPredPoint *pred_stats =
		GPOS_NEW(mp) CStatsPredPoint(col_ref->Id(), stats_cmp_type, point);

	return pred_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetStatsPredPoint
//
//	@doc:
//		Extract statistics filtering information from a point comparison
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetStatsPredPoint(CMemoryPool *mp, CExpression *predicate_expr,
								   CColRefSet *	 //outer_refs,
)
{
	GPOS_ASSERT(NULL != predicate_expr);

	CStatsPred *pred_stats = GetPredStats(mp, predicate_expr);
	GPOS_ASSERT(NULL != pred_stats);

	return pred_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetStatsCmpType
//
//	@doc:
// 		Return the statistics predicate comparison type
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::GetStatsCmpType(IMDId *mdid)
{
	GPOS_ASSERT(NULL != mdid);
	CStatsPred::EStatsCmpType stats_cmp_type = StatsCmpType(mdid);

	if (CStatsPred::EstatscmptOther != stats_cmp_type)
	{
		return stats_cmp_type;
	}

	if (CPredicateUtils::FLikePredicate(mdid))
	{
		return CStatsPred::EstatscmptLike;
	}

	return CStatsPred::EstatscmptOther;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetPredStats
//
//	@doc:
// 		Generate a statistics point predicate for expressions that are supported.
//		Else an unsupported predicate. Note that the comparison semantics extracted are
//		for statistics computation only.
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetPredStats(CMemoryPool *mp, CExpression *expr)
{
	GPOS_ASSERT(NULL != expr);

	CExpression *expr_left = NULL;
	CExpression *expr_right = NULL;

	CStatsPred::EStatsCmpType stats_cmp_type = CStatsPred::EstatscmptOther;
	if (CPredicateUtils::FIdentIDFConstIgnoreCast(expr))
	{
		expr_left = (*expr)[0];
		expr_right = (*expr)[1];

		stats_cmp_type = CStatsPred::EstatscmptIDF;
	}
	else if (CPredicateUtils::FIdentINDFConstIgnoreCast(expr))
	{
		CExpression *expr_child = (*expr)[0];
		expr_left = (*expr_child)[0];
		expr_right = (*expr_child)[1];

		stats_cmp_type = CStatsPred::EstatscmptINDF;
	}
	else
	{
		expr_left = (*expr)[0];
		expr_right = (*expr)[1];

		GPOS_ASSERT(CPredicateUtils::FIdentCompareConstIgnoreCast(
			expr, COperator::EopScalarCmp));

		COperator *expr_operator = expr->Pop();
		CScalarCmp *scalar_cmp_op = CScalarCmp::PopConvert(expr_operator);

		// Comparison semantics for statistics purposes is looser
		// than regular comparison
		stats_cmp_type = GetStatsCmpType(scalar_cmp_op->MdIdOp());
	}

	GPOS_ASSERT(COperator::EopScalarIdent == expr_left->Pop()->Eopid() ||
				CScalarIdent::FCastedScId(expr_left));
	GPOS_ASSERT(COperator::EopScalarConst == expr_right->Pop()->Eopid() ||
				CScalarConst::FCastedConst(expr_right));

	const CColRef *col_ref =
		CCastUtils::PcrExtractFromScIdOrCastScId(expr_left);
	CScalarConst *scalar_const_op =
		CScalarConst::PopExtractFromConstOrCastConst(expr_right);
	GPOS_ASSERT(NULL != scalar_const_op);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	IDatum *datum = scalar_const_op->GetDatum();
	const IMDType *datum_type = md_accessor->RetrieveType(datum->MDId());

	BOOL is_text_related =
		datum_type->IsTextRelated() && col_ref->RetrieveType()->IsTextRelated();
	if (is_text_related &&
		!CHistogram::IsOpSupportedForTextFilter(stats_cmp_type))
	{
		return GPOS_NEW(mp)
			CStatsPredUnsupported(col_ref->Id(), stats_cmp_type);
	}


	if (!CHistogram::IsOpSupportedForFilter(stats_cmp_type) ||
		!IMDType::StatsAreComparable(col_ref->RetrieveType(), datum))
	{
		// case 1: unsupported predicate for stats calculations
		// example: SELECT 1 FROM pg_catalog.pg_class c WHERE c.relname ~ '^(t36)$';
		// case 2: unsupported stats comparison between the column and datum

		return GPOS_NEW(mp)
			CStatsPredUnsupported(col_ref->Id(), stats_cmp_type);
	}

	return GPOS_NEW(mp) CStatsPredPoint(mp, col_ref, stats_cmp_type, datum);
}


//---------------------------------------------------------------------------
//		CStatsPredUtils::IsJoinPredSupportedForStatsEstimation
//
//		Given a join predicate <expr>, return whether this is a supported
//		join predicate for cardinality estimation, and what method to use
//		to build the join statistics.
//
//		Also return ColRefs for those sides of the comparison predicate that
//		can be used (either the entire histogram or just the NDV).
//
//		Supported predicates:
//
//		All of these must reference the outer table only on one side
//		and the inner table only on the other side.
//
//		col1 <op> col2          (op could be INDF, IDF, =, <, <=, >, >=, <>)
//		col1 = p(col2)          (p is an NDV-preserving function)
//		p(col1) = p(col2)
//		col1 = expr(col2...coln)
//		p(col1) = expr(col2...coln)
//
//		plus variations of the above, flipping sides and adding casts.
//		Non-NDV-preserving expressions are not allowed on the inner side
//		of semi and anti-semijoins because we need the NDV of the join column
//		for those (LOJ stats are calculated using a semi-join, so the
//		restriction affects those as well).
//
//		For all but the first line above, we use an NDV-based stats method.
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsJoinPredSupportedForStatsEstimation(
	CExpression *expr,
	CColRefSetArray *
		output_col_refsets,	 // array of output columns of join's relational inputs
	BOOL is_semi_or_anti_join, CStatsPred::EStatsCmpType *stats_pred_cmp_type,
	const CColRef **col_ref_outer, const CColRef **col_ref_inner)
{
	GPOS_ASSERT(NULL != col_ref_outer);
	GPOS_ASSERT(NULL != col_ref_inner);
	GPOS_ASSERT(NULL == *col_ref_outer);
	GPOS_ASSERT(NULL == *col_ref_inner);
	COperator *expr_op = expr->Pop();

	BOOL is_INDF = CPredicateUtils::FINDF(expr);
	BOOL is_IDF = CPredicateUtils::FIDF(expr);
	BOOL is_scalar_cmp = (COperator::EopScalarCmp == expr_op->Eopid());
	// left and right children of our join pred operator
	CExpression *expr_left = NULL;
	CExpression *expr_right = NULL;

	// initialize output parameters
	*col_ref_inner = NULL;
	*col_ref_outer = NULL;

	if (!is_scalar_cmp && !is_INDF && !is_IDF)
	{
		// an unsupported expression
		*stats_pred_cmp_type = CStatsPred::EstatscmptOther;
		return false;
	}

	if (is_INDF)
	{
		(*stats_pred_cmp_type) = CStatsPred::EstatscmptINDF;
		CExpression *expr_IDF = (*expr)[0];
		expr_left = (*expr_IDF)[0];
		expr_right = (*expr_IDF)[1];
	}
	else if (is_IDF)
	{
		(*stats_pred_cmp_type) = CStatsPred::EstatscmptIDF;
		expr_left = (*expr)[0];
		expr_right = (*expr)[1];
	}
	else
	{
		GPOS_ASSERT(is_scalar_cmp);

		CScalarCmp *sc_cmp_op = CScalarCmp::PopConvert(expr_op);

		// Comparison semantics for stats purposes is looser
		// than regular comparison.
		(*stats_pred_cmp_type) =
			CStatsPredUtils::StatsCmpType(sc_cmp_op->MdIdOp());

		expr_left = (*expr)[0];
		expr_right = (*expr)[1];
	}

	// expr_left and expr_right associated with the outer and inner tables
	CExpression *assigned_expr_outer = NULL;
	CExpression *assigned_expr_inner = NULL;

	if (!AssignExprsToOuterAndInner(output_col_refsets, expr_left, expr_right,
									&assigned_expr_outer, &assigned_expr_inner))
	{
		// we are not dealing with a join predicate where one side of the operator
		// refers to the outer table and the other side refers to the inner
		return false;
	}

	// check whether left or right expressions are simple columns or casts
	// of simple columns
	(*col_ref_outer) =
		CCastUtils::PcrExtractFromScIdOrCastScId(assigned_expr_outer);
	(*col_ref_inner) =
		CCastUtils::PcrExtractFromScIdOrCastScId(assigned_expr_inner);

	if (NULL != *col_ref_outer && NULL != *col_ref_inner)
	{
		// a simple predicate of the form col1 <op> col2 (casts are allowed)
		return true;
	}

	// if the scalar cmp is of equality type, we may not have been able to extract
	// the column references of scalar ident if they had any other expression than cast
	// on top of them.
	// in such cases, check if there is still a possibility to extract scalar ident,
	// if there is more than one column reference on either side, this is unsupported
	// If supported, mark the comparison as NDV-based

	if (*stats_pred_cmp_type == CStatsPred::EstatscmptEq)
	{
		BOOL outer_is_ndv_preserving =
			(NULL != *col_ref_outer ||
			 CUtils::IsExprNDVPreserving(assigned_expr_outer, col_ref_outer));
		BOOL inner_is_ndv_preserving =
			(NULL != *col_ref_inner ||
			 CUtils::IsExprNDVPreserving(assigned_expr_inner, col_ref_inner));

		if (!outer_is_ndv_preserving && !inner_is_ndv_preserving)
		{
			// join pred of the form f(a) = f(b) with neither side NDV-preserving, this is not supported
			return false;
		}

		if (is_semi_or_anti_join && !inner_is_ndv_preserving)
		{
			// non-NDV-preserving functions on the inner of a semi-join or anti-semijoin
			// are not supported, we need the NDV of the inner join columns to calculate
			// the stats
			return false;
		}

		// a join predicate that involves an NDV-preserving function on at least one side, one of
		// *col_ref_inner and *col_ref_outer may be NULL. If expr(...) is a non-NDV-preserving
		// expression and p is an NDV-preserving function, then we can have one of the following
		// (including variations with flipped sides and casts added):
		// col1 = p(col2)                (use max of both NDVs)
		// p(col1) = p(col2)             (use max of both NDVs)
		// col1 = expr(col2...coln)      (use NDV of col1)
		// p(col1) = expr(col2...coln)   (use NDV of col1)
		*stats_pred_cmp_type = CStatsPred::EstatscmptEqNDV;
		return true;
	}

	// failed to extract a scalar ident
	return false;
}


BOOL
CStatsPredUtils::AssignExprsToOuterAndInner(
	CColRefSetArray *
		output_col_refsets,	 // array of output columns of join's relational inputs
	CExpression *expr_1, CExpression *expr_2, CExpression **outer_expr,
	CExpression **inner_expr)
{
	// see also CPhysicalJoin::FPredKeysSeparated(), which returns similar info
	CColRefSet *used_cols_1 = expr_1->DeriveUsedColumns();
	CColRefSet *used_cols_2 = expr_2->DeriveUsedColumns();
	ULONG child_index_1 = 0;
	ULONG child_index_2 = 0;

	if (0 == used_cols_1->Size() || 0 == used_cols_2->Size())
	{
		// one of the sides is a constant
		return false;
	}

	// try just one ColRef from each side and find the associated input table
	child_index_1 = CUtils::UlPcrIndexContainingSet(output_col_refsets,
													used_cols_1->PcrAny());
	child_index_2 = CUtils::UlPcrIndexContainingSet(output_col_refsets,
													used_cols_2->PcrAny());

	if (gpos::ulong_max == child_index_1 || gpos::ulong_max == child_index_2)
	{
		// the predicate refers to columns that are not available
		// (predicate from NAry join that refers to tables not yet being processed)
		return false;
	}
	if (child_index_1 == child_index_2)
	{
		// both sides refer to the same input table
		return false;
	}

	// we tried one ColRef above, now try all of them, if there are multiple
	if ((1 < used_cols_1->Size() &&
		 !(*output_col_refsets)[child_index_1]->ContainsAll(used_cols_1)) ||
		(1 < used_cols_2->Size() &&
		 !(*output_col_refsets)[child_index_2]->ContainsAll(used_cols_2)))
	{
		// at least one of the sides refers to more than one input table
		return false;
	}

	if (child_index_1 < child_index_2)
	{
		GPOS_ASSERT(0 == child_index_1 && 1 == child_index_2);
		*outer_expr = expr_1;
		*inner_expr = expr_2;
	}
	else
	{
		GPOS_ASSERT(0 == child_index_2 && 1 == child_index_1);
		*outer_expr = expr_2;
		*inner_expr = expr_1;
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractPredStats
//
//	@doc:
//		Extract scalar expression for statistics filtering
//
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::ExtractPredStats(CMemoryPool *mp, CExpression *scalar_expr,
								  CColRefSet *outer_refs)
{
	GPOS_ASSERT(NULL != scalar_expr);
	if (CPredicateUtils::FOr(scalar_expr))
	{
		CStatsPred *disjunctive_pred_stats =
			CreateStatsPredDisj(mp, scalar_expr, outer_refs);
		if (NULL != disjunctive_pred_stats)
		{
			return disjunctive_pred_stats;
		}
	}
	else
	{
		CStatsPred *conjunctive_pred_stats =
			CreateStatsPredConj(mp, scalar_expr, outer_refs);
		if (NULL != conjunctive_pred_stats)
		{
			return conjunctive_pred_stats;
		}
	}

	return GPOS_NEW(mp) CStatsPredConj(GPOS_NEW(mp) CStatsPredPtrArry(mp));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::CreateStatsPredConj
//
//	@doc:
//		Create conjunctive statistics filter composed of the extracted
//		components of the conjunction
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::CreateStatsPredConj(CMemoryPool *mp, CExpression *scalar_expr,
									 CColRefSet *outer_refs)
{
	GPOS_ASSERT(NULL != scalar_expr);
	CExpressionArray *pred_expr_conjuncts =
		CPredicateUtils::PdrgpexprConjuncts(mp, scalar_expr);
	const ULONG size = pred_expr_conjuncts->Size();

	CStatsPredPtrArry *pred_stats_array = GPOS_NEW(mp) CStatsPredPtrArry(mp);
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *predicate_expr = (*pred_expr_conjuncts)[ul];
		CColRefSet *col_refset_used = predicate_expr->DeriveUsedColumns();
		if (NULL != outer_refs && outer_refs->ContainsAll(col_refset_used))
		{
			// skip predicate with outer references
			continue;
		}

		if (CPredicateUtils::FOr(predicate_expr))
		{
			CStatsPred *disjunctive_pred_stats =
				CreateStatsPredDisj(mp, predicate_expr, outer_refs);
			if (NULL != disjunctive_pred_stats)
			{
				pred_stats_array->Append(disjunctive_pred_stats);
			}
		}
		else
		{
			AddSupportedStatsFilters(mp, pred_stats_array, predicate_expr,
									 outer_refs);
		}
	}

	pred_expr_conjuncts->Release();

	if (0 < pred_stats_array->Size())
	{
		return GPOS_NEW(mp) CStatsPredConj(pred_stats_array);
	}

	pred_stats_array->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::CreateStatsPredDisj
//
//	@doc:
//		Create disjunctive statistics filter composed of the extracted
//		components of the disjunction
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::CreateStatsPredDisj(CMemoryPool *mp,
									 CExpression *predicate_expr,
									 CColRefSet *outer_refs)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(CPredicateUtils::FOr(predicate_expr));

	CStatsPredPtrArry *pred_stats_disj_child =
		GPOS_NEW(mp) CStatsPredPtrArry(mp);

	// remove duplicate components of the OR tree
	CExpression *expr_copy =
		CExpressionUtils::PexprDedupChildren(mp, predicate_expr);

	// extract the components of the OR tree
	CExpressionArray *disjunct_expr =
		CPredicateUtils::PdrgpexprDisjuncts(mp, expr_copy);
	const ULONG size = disjunct_expr->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *expr = (*disjunct_expr)[ul];
		CColRefSet *col_refset_used = expr->DeriveUsedColumns();
		if (NULL != outer_refs && outer_refs->ContainsAll(col_refset_used))
		{
			// skip predicate with outer references
			continue;
		}

		AddSupportedStatsFilters(mp, pred_stats_disj_child, expr, outer_refs);
	}

	// clean up
	expr_copy->Release();
	disjunct_expr->Release();

	if (0 < pred_stats_disj_child->Size())
	{
		return GPOS_NEW(mp) CStatsPredDisj(pred_stats_disj_child);
	}

	pred_stats_disj_child->Release();

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::AddSupportedStatsFilters
//
//	@doc:
//		Add supported filter for statistics computation
//---------------------------------------------------------------------------
void
CStatsPredUtils::AddSupportedStatsFilters(CMemoryPool *mp,
										  CStatsPredPtrArry *pred_stats_array,
										  CExpression *predicate_expr,
										  CColRefSet *outer_refs)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(NULL != pred_stats_array);

	CColRefSet *col_refset_used = predicate_expr->DeriveUsedColumns();
	if (NULL != outer_refs && outer_refs->ContainsAll(col_refset_used))
	{
		// skip predicates with outer references
		return;
	}

	if (COperator::EopScalarConst == predicate_expr->Pop()->Eopid())
	{
		pred_stats_array->Append(GPOS_NEW(mp) CStatsPredUnsupported(
			gpos::ulong_max, CStatsPred::EstatscmptOther,
			CHistogram::NeutralScaleFactor));

		return;
	}

	if (COperator::EopScalarArrayCmp == predicate_expr->Pop()->Eopid())
	{
		ProcessArrayCmp(mp, predicate_expr, pred_stats_array);
	}
	else
	{
		CStatsPredUtils::EPredicateType ept =
			GetPredTypeForExpr(mp, predicate_expr);
		GPOS_ASSERT(CStatsPredUtils::EptSentinel != ept);

		CStatsPred *pred_stats;
		switch (ept)
		{
			case CStatsPredUtils::EptDisj:
				pred_stats = CStatsPredUtils::CreateStatsPredDisj(
					mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptScIdent:
				pred_stats = CStatsPredUtils::GetStatsPredFromBoolExpr(
					mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptLike:
				pred_stats = CStatsPredUtils::GetStatsPredLike(
					mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptPoint:
				pred_stats = CStatsPredUtils::GetStatsPredPoint(
					mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptConj:
				pred_stats = CStatsPredUtils::CreateStatsPredConj(
					mp, predicate_expr, outer_refs);
				break;
			case CStatsPredUtils::EptNullTest:
				pred_stats = CStatsPredUtils::GetStatsPredNullTest(
					mp, predicate_expr, outer_refs);
				break;
			default:
				pred_stats = CStatsPredUtils::CreateStatsPredUnsupported(
					mp, predicate_expr, outer_refs);
				break;
		}

		if (NULL != pred_stats)
		{
			pred_stats_array->Append(pred_stats);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetPredTypeForExpr
//
//	@doc:
//		Return statistics filter type of the given expression
//---------------------------------------------------------------------------
CStatsPredUtils::EPredicateType
CStatsPredUtils::GetPredTypeForExpr(CMemoryPool *mp,
									CExpression *predicate_expr)
{
	GPOS_ASSERT(NULL != predicate_expr);
	if (CPredicateUtils::FOr(predicate_expr))
	{
		return CStatsPredUtils::EptDisj;
	}

	if (IsPredBooleanScIdent(predicate_expr))
	{
		return CStatsPredUtils::EptScIdent;
	}

	if (CPredicateUtils::FLikePredicate(predicate_expr))
	{
		return CStatsPredUtils::EptLike;
	}

	if (IsPointPredicate(predicate_expr))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (IsPredPointIDF(predicate_expr))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (IsPredPointINDF(predicate_expr))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (IsConjunction(mp, predicate_expr))
	{
		return CStatsPredUtils::EptConj;
	}

	if (IsPredScalarIdentIsNull(predicate_expr) ||
		IsPredScalarIdentIsNotNull(predicate_expr))
	{
		return CStatsPredUtils::EptNullTest;
	}

	return CStatsPredUtils::EptUnsupported;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsConjunction
//
//	@doc:
//		Is the condition a conjunctive predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsConjunction(CMemoryPool *mp, CExpression *predicate_expr)
{
	GPOS_ASSERT(NULL != predicate_expr);
	CExpressionArray *expr_conjuncts =
		CPredicateUtils::PdrgpexprConjuncts(mp, predicate_expr);
	const ULONG size = expr_conjuncts->Size();
	expr_conjuncts->Release();

	return (1 < size);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredBooleanScIdent
//
//	@doc:
//		Is the condition a boolean predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredBooleanScIdent(CExpression *predicate_expr)
{
	GPOS_ASSERT(NULL != predicate_expr);
	return CPredicateUtils::FBooleanScalarIdent(predicate_expr) ||
		   CPredicateUtils::FNegatedBooleanScalarIdent(predicate_expr);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPointPredicate
//
//	@doc:
//		Is the condition a point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPointPredicate(CExpression *predicate_expr)
{
	GPOS_ASSERT(NULL != predicate_expr);
	return (CPredicateUtils::FIdentCompareConstIgnoreCast(
		predicate_expr, COperator::EopScalarCmp));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredPointIDF
//
//	@doc:
//		Is the condition an IDF point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredPointIDF(CExpression *predicate_expr)
{
	GPOS_ASSERT(NULL != predicate_expr);
	return CPredicateUtils::FIdentIDFConstIgnoreCast(predicate_expr);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredPointINDF
//
//	@doc:
//		Is the condition an INDF point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredPointINDF(CExpression *predicate_expr)
{
	GPOS_ASSERT(NULL != predicate_expr);

	if (!CPredicateUtils::FNot(predicate_expr))
	{
		return false;
	}

	return IsPredPointIDF((*predicate_expr)[0]);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsPredScalarIdentIsNull
//
//	@doc:
//		Is the condition a 'is null' test on top a scalar ident
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredScalarIdentIsNull(CExpression *predicate_expr)
{
	GPOS_ASSERT(NULL != predicate_expr);

	if (0 == predicate_expr->Arity())
	{
		return false;
	}
	// currently we support null test on scalar ident only
	return CUtils::FScalarNullTest(predicate_expr) &&
		   CUtils::FScalarIdent((*predicate_expr)[0]);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FScalarIdentNullTest
//
//	@doc:
//		Is the condition a not-null test
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsPredScalarIdentIsNotNull(CExpression *predicate_expr)
{
	GPOS_ASSERT(NULL != predicate_expr);

	if (0 == predicate_expr->Arity())
	{
		return false;
	}
	CExpression *expr_null_test = (*predicate_expr)[0];

	// currently we support not-null test on scalar ident only
	return CUtils::FScalarBoolOp(predicate_expr, CScalarBoolOp::EboolopNot) &&
		   IsPredScalarIdentIsNull(expr_null_test);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredLikeHandleCasting
//
//	@doc:
//		Create a LIKE statistics filter
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetStatsPredLike(CMemoryPool *mp, CExpression *predicate_expr,
								  CColRefSet *	//outer_refs,
)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(CPredicateUtils::FLikePredicate(predicate_expr));

	CExpression *expr_left = (*predicate_expr)[0];
	CExpression *expr_right = (*predicate_expr)[1];

	// we support LIKE predicate of the following patterns
	// CAST(ScIdent) LIKE Const
	// CAST(ScIdent) LIKE CAST(Const)
	// ScIdent LIKE Const
	// ScIdent LIKE CAST(Const)
	// CAST(Const) LIKE ScIdent
	// CAST(Const) LIKE CAST(ScIdent)
	// const LIKE ScIdent
	// const LIKE CAST(ScIdent)

	CExpression *expr_scalar_ident = NULL;
	CExpression *expr_scalar_const = NULL;

	CPredicateUtils::ExtractLikePredComponents(
		predicate_expr, &expr_scalar_ident, &expr_scalar_const);

	if (NULL == expr_scalar_ident || NULL == expr_scalar_const)
	{
		return GPOS_NEW(mp)
			CStatsPredUnsupported(gpos::ulong_max, CStatsPred::EstatscmptLike);
	}

	CScalarIdent *scalar_ident_op =
		CScalarIdent::PopConvert(expr_scalar_ident->Pop());
	ULONG colid = scalar_ident_op->Pcr()->Id();

	CScalarConst *scalar_const_op =
		CScalarConst::PopConvert(expr_scalar_const->Pop());
	IDatum *datum_literal = scalar_const_op->GetDatum();

	const CColRef *col_ref = scalar_ident_op->Pcr();
	if (!IMDType::StatsAreComparable(col_ref->RetrieveType(), datum_literal))
	{
		// unsupported stats comparison between the column and datum
		return GPOS_NEW(mp)
			CStatsPredUnsupported(col_ref->Id(), CStatsPred::EstatscmptLike);
	}

	CDouble default_scale_factor(1.0);
	if (datum_literal->SupportsLikePredicate())
	{
		default_scale_factor = datum_literal->GetLikePredicateScaleFactor();
	}

	expr_left->AddRef();
	expr_right->AddRef();

	return GPOS_NEW(mp)
		CStatsPredLike(colid, expr_left, expr_right, default_scale_factor);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ProcessArrayCmp
//
//	@doc:
//		Extract statistics filtering information from scalar array comparison
//---------------------------------------------------------------------------
void
CStatsPredUtils::ProcessArrayCmp(CMemoryPool *mp, CExpression *predicate_expr,
								 CStatsPredPtrArry *result_pred_stats)
{
	GPOS_ASSERT(NULL != result_pred_stats);
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(2 == predicate_expr->Arity());

	CScalarArrayCmp *scalar_array_cmp_op =
		CScalarArrayCmp::PopConvert(predicate_expr->Pop());
	CExpression *expr_scalar_array =
		CUtils::PexprScalarArrayChild(predicate_expr);
	BOOL is_supported_array_cmp =
		CPredicateUtils::FCompareCastIdentToConstArray(predicate_expr) ||
		CPredicateUtils::FCompareScalarIdentToConstAndScalarIdentArray(
			predicate_expr);

	if (!is_supported_array_cmp)
	{
		// unsupported predicate for stats calculations
		result_pred_stats->Append(GPOS_NEW(mp) CStatsPredUnsupported(
			gpos::ulong_max, CStatsPred::EstatscmptOther));
		return;
	}

	// extract the colref used in the array predicate expr
	CExpression *expr_ident;
	CExpression *left_expr = (*predicate_expr)[0];
	if (CUtils::FScalarIdent(left_expr))
	{
		expr_ident = left_expr;
	}
	else
	{
		GPOS_ASSERT(CCastUtils::FBinaryCoercibleCast((*predicate_expr)[0]));
		expr_ident = (*left_expr)[0];
	}
	const CColRef *col_ref = CScalarIdent::PopConvert(expr_ident->Pop())->Pcr();


	// comparison semantics for statistics purposes is looser than regular comparison.
	CStatsPred::EStatsCmpType stats_cmp_type =
		GetStatsCmpType(scalar_array_cmp_op->MdIdOp());
	if (!CHistogram::IsOpSupportedForFilter(stats_cmp_type))
	{
		// unsupported predicate for stats calculations
		result_pred_stats->Append(
			GPOS_NEW(mp) CStatsPredUnsupported(col_ref->Id(), stats_cmp_type));
		return;
	}

	// Ok handle support cases
	CStatsPredPtrArry *pred_stats;
	CPointArray *points = NULL;
	BOOL is_array_cmp_any =
		(CScalarArrayCmp::EarrcmpAny == scalar_array_cmp_op->Earrcmpt());
	BOOL is_array_cmp_eq = (stats_cmp_type == CStatsPred::EstatscmptEq);

	if (is_array_cmp_any)
	{
		// in case of exprs of the form "a op ANY (ARRAY[...])", each element
		// must be OR-d. So use a different array to collect the stats that
		// will be later placed under a CStatsPredDisj.
		pred_stats = GPOS_NEW(mp) CStatsPredPtrArry(mp);

		// In case op is "=", ORCA supports a fast-path for deriving stats using
		// CStatsPredArrayCmp. Collect "points" from the array list to create that.
		// TODO: Support more cases in CStatsPredArrayCmp
		if (is_array_cmp_eq)
		{
			points = GPOS_NEW(mp) CPointArray(mp);
		}
	}
	else
	{
		GPOS_ASSERT(CScalarArrayCmp::EarrcmpAll ==
					scalar_array_cmp_op->Earrcmpt());
		// in case of exprs of the form "a op ALL (ARRAY[...])", each element
		// is implicitly AND-ed, and so can be directly added to result_pred_stats
		pred_stats = result_pred_stats;
	}

	const ULONG num_array_elems = CUtils::UlScalarArrayArity(expr_scalar_array);

	for (ULONG ul = 0; ul < num_array_elems; ++ul)
	{
		CExpression *child_expr =
			CUtils::PScalarArrayExprChildAt(mp, expr_scalar_array, ul);

		if (COperator::EopScalarConst == child_expr->Pop()->Eopid())
		{
			IDatum *datum =
				CScalarConst::PopConvert(child_expr->Pop())->GetDatum();

			if (!datum->StatsAreComparable(datum))
			{
				// stats calculations on such datums unsupported
				CStatsPred *child_pred_stats = GPOS_NEW(mp)
					CStatsPredUnsupported(col_ref->Id(), stats_cmp_type);
				pred_stats->Append(child_pred_stats);
			}
			else if (is_array_cmp_any && is_array_cmp_eq)
			{
				// fast-path using CStatsPredArrayCmp
				GPOS_ASSERT(points != NULL);
				datum->AddRef();
				CPoint *point = GPOS_NEW(mp) CPoint(datum);
				points->Append(point);
			}
			else
			{
				CStatsPred *child_pred_stats = GPOS_NEW(mp)
					CStatsPredPoint(mp, col_ref, stats_cmp_type, datum);
				pred_stats->Append(child_pred_stats);
			}
		}

		child_expr->Release();
	}

	if (is_array_cmp_any)
	{
		if (is_array_cmp_eq)
		{
			// "a = ANY (ARRAY[...])"
			CStatsPredArrayCmp *pred_stats_array_cmp = GPOS_NEW(mp)
				CStatsPredArrayCmp(col_ref->Id(), stats_cmp_type, points);
			pred_stats->Append(pred_stats_array_cmp);
		}

		// "a op ANY (ARRAY[...])"
		CStatsPredDisj *pred_stats_disj =
			GPOS_NEW(mp) CStatsPredDisj(pred_stats);
		result_pred_stats->Append(pred_stats_disj);
	}
	else
	{
		GPOS_ASSERT(CScalarArrayCmp::EarrcmpAll ==
					scalar_array_cmp_op->Earrcmpt());
		// "a op ALL (ARRAY[...])"
		// no additional work needed here, since we already added the preds to the result_pred_stats
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::GetStatsPredFromBoolExpr
//
//	@doc:
//		Extract statistics filtering information from boolean predicate
//		in the form of scalar id or negated scalar id
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::GetStatsPredFromBoolExpr(CMemoryPool *mp,
										  CExpression *predicate_expr,
										  CColRefSet *	//outer_refs
)
{
	GPOS_ASSERT(NULL != predicate_expr);
	GPOS_ASSERT(CPredicateUtils::FBooleanScalarIdent(predicate_expr) ||
				CPredicateUtils::FNegatedBooleanScalarIdent(predicate_expr));

	COperator *predicate_op = predicate_expr->Pop();

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	IDatum *datum = NULL;
	ULONG colid = gpos::ulong_max;

	if (CPredicateUtils::FBooleanScalarIdent(predicate_expr))
	{
		CScalarIdent *scalar_ident_op = CScalarIdent::PopConvert(predicate_op);
		datum = md_accessor->PtMDType<IMDTypeBool>()->CreateBoolDatum(
			mp, true /* fValue */, false /* is_null */);
		colid = scalar_ident_op->Pcr()->Id();
	}
	else
	{
		CExpression *child_expr = (*predicate_expr)[0];
		datum = md_accessor->PtMDType<IMDTypeBool>()->CreateBoolDatum(
			mp, false /* fValue */, false /* is_null */);
		colid = CScalarIdent::PopConvert(child_expr->Pop())->Pcr()->Id();
	}

	if (!datum->StatsAreComparable(datum))
	{
		// stats calculations on such datums unsupported
		datum->Release();

		return GPOS_NEW(mp)
			CStatsPredUnsupported(colid, CStatsPred::EstatscmptEq);
	}


	GPOS_ASSERT(NULL != datum && gpos::ulong_max != colid);

	return GPOS_NEW(mp) CStatsPredPoint(colid, CStatsPred::EstatscmptEq,
										GPOS_NEW(mp) CPoint(datum));
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractJoinStatsFromJoinPred
//
//	@doc:
//		Helper function to extract the statistics join filter from a given join predicate
//
//---------------------------------------------------------------------------
CStatsPredJoin *
CStatsPredUtils::ExtractJoinStatsFromJoinPred(
	CMemoryPool *mp, CExpression *join_pred_expr,
	CColRefSetArray *
		output_col_refsets,	 // array of output columns of join's relational inputs
	CColRefSet *outer_refs, BOOL is_semi_or_anti_join,
	CExpressionArray *unsupported_expr_array)
{
	GPOS_ASSERT(NULL != join_pred_expr);
	GPOS_ASSERT(NULL != output_col_refsets);
	GPOS_ASSERT(NULL != unsupported_expr_array);

	CColRefSet *col_refset_used = join_pred_expr->DeriveUsedColumns();

	if (outer_refs->ContainsAll(col_refset_used))
	{
		return NULL;
	}

	const CColRef *col_ref_outer = NULL;
	const CColRef *col_ref_inner = NULL;
	CStatsPred::EStatsCmpType stats_cmp_type = CStatsPred::EstatscmptOther;

	BOOL fSupportedScIdentComparison = IsJoinPredSupportedForStatsEstimation(
		join_pred_expr, output_col_refsets, is_semi_or_anti_join,
		&stats_cmp_type, &col_ref_outer, &col_ref_inner);
	if (fSupportedScIdentComparison &&
		CStatsPred::EstatscmptOther != stats_cmp_type)
	{
		if (NULL != col_ref_outer && NULL != col_ref_inner &&
			!IMDType::StatsAreComparable(col_ref_outer->RetrieveType(),
										 col_ref_inner->RetrieveType()))
		{
			// unsupported statistics comparison between the histogram boundaries of the columns
			join_pred_expr->AddRef();
			unsupported_expr_array->Append(join_pred_expr);
			return NULL;
		}

		ULONG outer_id =
			(NULL != col_ref_outer ? col_ref_outer->Id() : gpos::ulong_max);
		ULONG inner_id =
			(NULL != col_ref_inner ? col_ref_inner->Id() : gpos::ulong_max);

		return GPOS_NEW(mp) CStatsPredJoin(outer_id, stats_cmp_type, inner_id);
	}

	if (CColRefSet::FCovered(output_col_refsets, col_refset_used))
	{
		// unsupported join predicate
		join_pred_expr->AddRef();
		unsupported_expr_array->Append(join_pred_expr);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractJoinStatsFromJoinPredArray
//
//	@doc:
//		Helper function to extract array of statistics join filter
//		from an array of join predicates
//
//---------------------------------------------------------------------------
CStatsPredJoinArray *
CStatsPredUtils::ExtractJoinStatsFromJoinPredArray(
	CMemoryPool *mp, CExpression *scalar_expr,
	CColRefSetArray *
		output_col_refsets,	 // array of output columns of join's relational inputs
	CColRefSet *outer_refs, BOOL is_semi_or_antijoin,
	CStatsPred **unsupported_stats_pred_array)
{
	GPOS_ASSERT(NULL != scalar_expr);
	GPOS_ASSERT(NULL != output_col_refsets);

	CStatsPredJoinArray *join_preds_stats =
		GPOS_NEW(mp) CStatsPredJoinArray(mp);

	CExpressionArray *unsupported_expr_array =
		GPOS_NEW(mp) CExpressionArray(mp);

	// extract all the conjuncts
	CExpressionArray *expr_conjuncts =
		CPredicateUtils::PdrgpexprConjuncts(mp, scalar_expr);
	const ULONG size = expr_conjuncts->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CExpression *predicate_expr = (*expr_conjuncts)[ul];
		CStatsPredJoin *join_stats = ExtractJoinStatsFromJoinPred(
			mp, predicate_expr, output_col_refsets, outer_refs,
			is_semi_or_antijoin, unsupported_expr_array);
		if (NULL != join_stats)
		{
			join_preds_stats->Append(join_stats);
		}
	}

	const ULONG unsupported_pred_count = unsupported_expr_array->Size();
	if (1 == unsupported_pred_count)
	{
		*unsupported_stats_pred_array = CStatsPredUtils::ExtractPredStats(
			mp, (*unsupported_expr_array)[0], outer_refs);
	}
	else if (1 < unsupported_pred_count)
	{
		unsupported_expr_array->AddRef();
		CExpression *expr_conjunct = CPredicateUtils::PexprConjDisj(
			mp, unsupported_expr_array, true /* fConjunction */);
		*unsupported_stats_pred_array =
			CStatsPredUtils::ExtractPredStats(mp, expr_conjunct, outer_refs);
		expr_conjunct->Release();
	}

	// clean up
	unsupported_expr_array->Release();
	expr_conjuncts->Release();

	return join_preds_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractJoinStatsFromExpr
//
//	@doc:
//		Helper function to extract array of statistics join filter from
//		an expression
//---------------------------------------------------------------------------
CStatsPredJoinArray *
CStatsPredUtils::ExtractJoinStatsFromExpr(
	CMemoryPool *mp, CExpressionHandle &expr_handle,
	CExpression *pexprScalarInput,
	CColRefSetArray *
		output_col_refsets,	 // array of output columns of join's relational inputs
	CColRefSet *outer_refs, BOOL is_semi_or_anti_join)
{
	GPOS_ASSERT(NULL != output_col_refsets);

	// remove implied predicates from join condition to avoid cardinality under-estimation
	CExpression *scalar_expr = CPredicateUtils::PexprRemoveImpliedConjuncts(
		mp, pexprScalarInput, expr_handle);

	// extract all the conjuncts
	CStatsPred *unsupported_pred_stats = NULL;
	CStatsPredJoinArray *join_pred_stats = ExtractJoinStatsFromJoinPredArray(
		mp, scalar_expr, output_col_refsets, outer_refs, is_semi_or_anti_join,
		&unsupported_pred_stats);

	// TODO:  May 15 2014, handle unsupported predicates for LASJ, LOJ and LS joins
	// clean up
	CRefCount::SafeRelease(unsupported_pred_stats);
	scalar_expr->Release();

	return join_pred_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ExtractJoinStatsFromExprHandle
//
//	@doc:
//		Extract statistics join information
//
//---------------------------------------------------------------------------
CStatsPredJoinArray *
CStatsPredUtils::ExtractJoinStatsFromExprHandle(CMemoryPool *mp,
												CExpressionHandle &expr_handle,
												BOOL is_semi_or_anti_join)
{
	// in case of subquery in join predicate, we return empty stats
	if (expr_handle.DeriveHasSubquery(expr_handle.Arity() - 1))
	{
		return GPOS_NEW(mp) CStatsPredJoinArray(mp);
	}

	CColRefSetArray *output_col_refsets = GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG size = expr_handle.Arity();
	for (ULONG ul = 0; ul < size - 1; ul++)
	{
		CColRefSet *output_col_ref_set = expr_handle.DeriveOutputColumns(ul);
		output_col_ref_set->AddRef();
		output_col_refsets->Append(output_col_ref_set);
	}

	// TODO:  02/29/2012 replace with constraint property info once available
	CExpression *scalar_expr =
		expr_handle.PexprScalarRepChild(expr_handle.Arity() - 1);
	CColRefSet *outer_refs = expr_handle.DeriveOuterReferences();

	CStatsPredJoinArray *join_pred_stats = ExtractJoinStatsFromExpr(
		mp, expr_handle, scalar_expr, output_col_refsets, outer_refs,
		is_semi_or_anti_join);

	// clean up
	output_col_refsets->Release();

	return join_pred_stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsConjOrDisjPred
//
//	@doc:
//		Is the predicate a conjunctive or disjunctive predicate
//
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsConjOrDisjPred(CStatsPred *pred_stats)
{
	return ((CStatsPred::EsptConj == pred_stats->GetPredStatsType()) ||
			(CStatsPred::EsptDisj == pred_stats->GetPredStatsType()));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::IsUnsupportedPredOnDefinedCol
//
//	@doc:
//		Is unsupported predicate on defined column
//
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::IsUnsupportedPredOnDefinedCol(CStatsPred *pred_stats)
{
	return ((CStatsPred::EsptUnsupported == pred_stats->GetPredStatsType()) &&
			(gpos::ulong_max == pred_stats->GetColId()));
}