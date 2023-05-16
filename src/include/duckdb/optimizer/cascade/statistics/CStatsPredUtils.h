//---------------------------------------------------------------------------
//	@filename:
//		CStatsPredUtils.h
//
//	@doc:
//		Utility functions for extracting statistics predicates
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatsPredUtils_H
#define GPOPT_CStatsPredUtils_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPred.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPredUtils
//
//	@doc:
//		Utility functions for extracting statistics predicates
//
//---------------------------------------------------------------------------
class CStatsPredUtils
{
private:
	// type of predicate expression
	enum EPredicateType
	{
		EptPoint,  // filter with literals
		EptScIdent,	 // scalar ident that is convert into an equals true/false filter
		EptConj,		 // conjunctive filter
		EptDisj,		 // disjunctive filter
		EptLike,		 // LIKE filter
		EptNullTest,	 // null test, including IS NULL and IS NOT NULL
		EptUnsupported,	 // unsupported filter for statistics calculation

		EptSentinel
	};

	// return the comparison type of an operator for the purpose of statistics computation
	static CStatsPred::EStatsCmpType StatsCmpType(IMDId *mdid);

	// return the comparison type of an operator for the purpose of statistics computation
	static CStatsPred::EStatsCmpType StatsCmpType(
		const CWStringConst *str_opname);

	// extract statistics filtering information from boolean expression
	static CStatsPred *GetStatsPredFromBoolExpr(CMemoryPool *mp,
												CExpression *predicate_expr,
												CColRefSet *outer_refs);

	// extract statistics filtering information from a scalar array compare operator
	static void ProcessArrayCmp(CMemoryPool *mp, CExpression *predicate_expr,
								CStatsPredPtrArry *pdrgpstatspred);

	// create and add statistics filtering information for supported filters
	static void AddSupportedStatsFilters(CMemoryPool *mp,
										 CStatsPredPtrArry *pdrgpstatspred,
										 CExpression *predicate_expr,
										 CColRefSet *outer_refs);

	// create a conjunctive statistics filter composed of the extracted components of the conjunction
	static CStatsPred *CreateStatsPredConj(CMemoryPool *mp,
										   CExpression *scalar_expr,
										   CColRefSet *outer_refs);

	// create a disjunction statistics filter composed of the extracted components of the disjunction
	static CStatsPred *CreateStatsPredDisj(CMemoryPool *mp,
										   CExpression *predicate_expr,
										   CColRefSet *outer_refs);

	// return statistics filter type for the given expression
	static CStatsPredUtils::EPredicateType GetPredTypeForExpr(
		CMemoryPool *mp, CExpression *predicate_expr);

	// is the condition a conjunctive predicate
	static BOOL IsConjunction(CMemoryPool *mp, CExpression *predicate_expr);

	// is the condition a boolean predicate
	static BOOL IsPredBooleanScIdent(CExpression *predicate_expr);

	// is the condition a point predicate
	static BOOL IsPointPredicate(CExpression *predicate_expr);

	// is the condition an IDF point predicate
	static BOOL IsPredPointIDF(CExpression *predicate_expr);

	// is the condition an INDF point predicate
	static BOOL IsPredPointINDF(CExpression *predicate_expr);

	// is the condition 'is null' on a scalar ident
	static BOOL IsPredScalarIdentIsNull(CExpression *predicate_expr);

	// is the condition 'is not null' on a scalar ident
	static BOOL IsPredScalarIdentIsNotNull(CExpression *predicate_expr);

	// extract statistics filtering information from a point comparison
	static CStatsPred *GetStatsPredPoint(CMemoryPool *mp,
										 CExpression *predicate_expr,
										 CColRefSet *outer_refs);

	// extract statistics filtering information from a LIKE comparison
	static CStatsPred *GetStatsPredLike(CMemoryPool *mp,
										CExpression *predicate_expr,
										CColRefSet *outer_refs);

	// extract statistics filtering information from a null test
	static CStatsPred *GetStatsPredNullTest(CMemoryPool *mp,
											CExpression *predicate_expr,
											CColRefSet *outer_refs);

	// create an unsupported statistics predicate
	static CStatsPred *CreateStatsPredUnsupported(CMemoryPool *mp,
												  CExpression *predicate_expr,
												  CColRefSet *outer_refs);

	// generate a point predicate for expressions of the form colid CMP constant for which we support stats calculation;
	// else return an unsupported stats predicate
	static CStatsPred *GetPredStats(CMemoryPool *mp, CExpression *expr);

	// return the statistics predicate comparison type based on the md identifier
	static CStatsPred::EStatsCmpType GetStatsCmpType(IMDId *mdid);

	// helper function to extract statistics join filter from a given join predicate
	static CStatsPredJoin *ExtractJoinStatsFromJoinPred(
		CMemoryPool *mp, CExpression *join_predicate_expr,
		CColRefSetArray *
			join_output_col_refset,	 // array of output columns of join's relational inputs
		CColRefSet *outer_refs, BOOL is_semi_or_anti_join,
		CExpressionArray *unsupported_predicates_expr);

	// Is the expression a comparison of scalar idents (or casted scalar idents),
	// or of other supported expressions? If so, extract relevant info.
	static BOOL IsJoinPredSupportedForStatsEstimation(
		CExpression *expr,
		CColRefSetArray *
			output_col_refsets,	 // array of output columns of join's relational inputs
		BOOL is_semi_or_anti_join,
		CStatsPred::EStatsCmpType *stats_pred_cmp_type,
		const CColRef **col_ref_outer, const CColRef **col_ref_inner);

	// find out which input expression refers only to the inner table and which
	// refers only to the outer table, and return accordingly
	static BOOL AssignExprsToOuterAndInner(
		CColRefSetArray *
			output_col_refsets,	 // array of output columns of join's relational inputs
		CExpression *expr_1, CExpression *expr_2, CExpression **outer_expr,
		CExpression **inner_expr);

public:
	// extract statistics filter from scalar expression
	static CStatsPred *ExtractPredStats(CMemoryPool *mp,
										CExpression *scalar_expr,
										CColRefSet *outer_refs);

	// helper function to extract array of statistics join filter from an array of join predicates
	static CStatsPredJoinArray *ExtractJoinStatsFromJoinPredArray(
		CMemoryPool *mp, CExpression *scalar_expr,
		CColRefSetArray *
			output_col_refset,	// array of output columns of join's relational inputs
		CColRefSet *outer_refs, BOOL is_semi_or_anti_join,
		CStatsPred **unsupported_pred_stats);

	// helper function to extract array of statistics join filter from an expression handle
	static CStatsPredJoinArray *ExtractJoinStatsFromExprHandle(
		CMemoryPool *mp, CExpressionHandle &expr_handle,
		BOOL is_semi_or_anti_join);

	// helper function to extract array of statistics join filter from an expression
	static CStatsPredJoinArray *ExtractJoinStatsFromExpr(
		CMemoryPool *mp, CExpressionHandle &expr_handle,
		CExpression *scalar_expression, CColRefSetArray *output_col_refset,
		CColRefSet *outer_refs, BOOL is_semi_or_anti_join);

	// is the predicate a conjunctive or disjunctive predicate
	static BOOL IsConjOrDisjPred(CStatsPred *pred_stats);

	// is unsupported predicate on unknown column
	static BOOL IsUnsupportedPredOnDefinedCol(CStatsPred *pred_stats);

};	// class CStatsPredUtils
}  // namespace gpopt

#endif