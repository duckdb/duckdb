//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2019 Pivotal Inc.
//
//	@filename:
//		CJoinOrderDPv2.cpp
//
//	@doc:
//		Implementation of dynamic programming-based join order generation
//---------------------------------------------------------------------------

#include "gpopt/xforms/CJoinOrderDPv2.h"

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/cost/ICostModelParams.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarNAryJoinPredList.h"
#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/IMDRelStats.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"


using namespace gpopt;

// how many expressions will we return at the end of the DP phase?
// Note that this includes query, mincard and greedy solutions.
#define GPOPT_DPV2_JOIN_ORDERING_TOPK 10
// cost penalty (a factor) for cross product for enumeration algorithms other than GreedyAvoidXProd
// (value determined by simple experiments on TPC-DS queries)
// This is the default value for optimizer_nestloop_factor
#define GPOPT_DPV2_CROSS_JOIN_DEFAULT_PENALTY 1024
// prohibitively high penalty for cross products when in GreedyAvoidXProd
#define GPOPT_DPV2_CROSS_JOIN_GREEDY_PENALTY 1e9

// from cost model used during optimization in CCostModelParamsGPDB.cpp
#define BCAST_SEND_COST 4.965e-05
#define BCAST_RECV_COST 1.35e-06
#define SEQ_SCAN_COST 5.50e-07
//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::CJoinOrderDPv2
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::CJoinOrderDPv2(CMemoryPool *mp,
							   CExpressionArray *pdrgpexprAtoms,
							   CExpressionArray *innerJoinConjuncts,
							   CExpressionArray *onPredConjuncts,
							   ULongPtrArray *childPredIndexes,
							   CColRefSet *outerRefs)
	: CJoinOrder(mp, pdrgpexprAtoms, innerJoinConjuncts, onPredConjuncts,
				 childPredIndexes),
	  m_expression_to_edge_map(NULL),
	  m_on_pred_conjuncts(onPredConjuncts),
	  m_child_pred_indexes(childPredIndexes),
	  m_non_inner_join_dependencies(NULL),
	  m_cross_prod_penalty(GPOPT_DPV2_CROSS_JOIN_DEFAULT_PENALTY),
	  m_outer_refs(outerRefs)
{
	m_join_levels = GPOS_NEW(mp) DPv2Levels(mp, m_ulComps + 1);
	// populate levels array with n+1 levels for an n-way join
	// level 0 remains unused, so index i corresponds to level i,
	// making it easier for humans to read the code
	for (ULONG l = 0; l <= m_ulComps; l++)
	{
		m_join_levels->Append(
			GPOS_NEW(mp) SLevelInfo(l, GPOS_NEW(mp) SGroupInfoArray(mp)));
	}

	m_bitset_to_group_info_map = GPOS_NEW(mp) BitSetToGroupInfoMap(mp);

	// Contains top k expressions for a general DP algorithm, without considering cost of motions/PS
	m_top_k_expressions =
		GPOS_NEW(mp) KHeap<SExpressionInfoArray, SExpressionInfo>(
			mp, GPOPT_DPV2_JOIN_ORDERING_TOPK);

	// We use a separate heap to ensure we produce an alternative expression that contains a dynamic PS
	// If no dynamic PS is valid, this will be empty.
	m_top_k_part_expressions =
		GPOS_NEW(mp) KHeap<SExpressionInfoArray, SExpressionInfo>(
			mp, 1 /* keep top 1 expression */
		);

	m_mp = mp;
	if (0 < m_on_pred_conjuncts->Size())
	{
		// we have non-inner joins, add dependency info
		ULONG numNonInnerJoins = m_on_pred_conjuncts->Size();

		m_non_inner_join_dependencies =
			GPOS_NEW(mp) CBitSetArray(mp, numNonInnerJoins);
		for (ULONG ul = 0; ul < numNonInnerJoins; ul++)
		{
			m_non_inner_join_dependencies->Append(GPOS_NEW(mp) CBitSet(mp));
		}

		// compute dependencies of the NIJ right children
		// (those components must appear on the left of the NIJ)
		// Note: NIJ = Non-inner join, e.g. LOJ
		for (ULONG en = 0; en < m_ulEdges; en++)
		{
			SEdge *pedge = m_rgpedge[en];

			if (0 < pedge->m_loj_num)
			{
				// edge represents a non-inner join pred
				ULONG logicalChildNum =
					FindLogicalChildByNijId(pedge->m_loj_num);
				CBitSet *nijBitSet =
					(*m_non_inner_join_dependencies)[pedge->m_loj_num - 1];

				GPOS_ASSERT(0 < logicalChildNum);
				nijBitSet->Union(pedge->m_pbs);
				// clear the bit representing the right side of the NIJ, we only
				// want to track the components needed on the left side
				nijBitSet->ExchangeClear(logicalChildNum);
			}
		}
	}
	PopulateExpressionToEdgeMapIfNeeded();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::~CJoinOrderDPv2
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::~CJoinOrderDPv2()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-allocations enabled in debug-build to detect any possible leaks
	CRefCount::SafeRelease(m_non_inner_join_dependencies);
	CRefCount::SafeRelease(m_child_pred_indexes);
	m_bitset_to_group_info_map->Release();
	CRefCount::SafeRelease(m_expression_to_edge_map);
	m_top_k_expressions->Release();
	m_top_k_part_expressions->Release();
	m_join_levels->Release();
	m_on_pred_conjuncts->Release();
	m_outer_refs->Release();
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::ComputeCost
//
//	@doc:
//		Primitive costing of join expressions;
//		Cost of a join expression is the "internal data flow" of the join
//		tree, the sum of all the rows flowing from the leaf nodes up to
//		the root.
//		NOTE: We could consider the width of the rows as well, if we had
//		a reliable way of determining the actual width.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::ComputeCost(SExpressionInfo *expr_info,
							CDouble join_cardinality)
{
	// cardinality of the expression itself is one part of the cost
	CDouble dCost(join_cardinality);

	if (expr_info->m_left_child_expr.IsValid())
	{
		GPOS_ASSERT(expr_info->m_right_child_expr.IsValid());
		// add cardinalities of the children to the cost
		dCost = dCost + expr_info->m_left_child_expr.GetExprInfo()->m_cost;
		dCost = dCost + expr_info->m_right_child_expr.GetExprInfo()->m_cost;

		if (CUtils::FCrossJoin(expr_info->m_expr))
		{
			// penalize cross joins, similar to what we do in the optimization phase
			dCost = dCost * m_cross_prod_penalty;
		}
		expr_info->m_cost_adj_PS =
			expr_info->m_cost_adj_PS +
			expr_info->m_left_child_expr.GetExprInfo()->m_cost_adj_PS;
		expr_info->m_cost_adj_PS =
			expr_info->m_cost_adj_PS +
			expr_info->m_right_child_expr.GetExprInfo()->m_cost_adj_PS;
	}

	expr_info->m_cost = dCost;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PexprBuildInnerJoinPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::PexprBuildInnerJoinPred(CBitSet *pbsFst, CBitSet *pbsSnd)
{
	GPOS_ASSERT(pbsFst->IsDisjoint(pbsSnd));
	// collect edges connecting the given sets
	CBitSet *pbsEdges = GPOS_NEW(m_mp) CBitSet(m_mp);
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp, *pbsFst);
	pbs->Union(pbsSnd);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (
			// edge represents an inner join pred
			0 == pedge->m_loj_num &&
			// all columns referenced in the edge pred are provided
			pbs->ContainsAll(pedge->m_pbs) &&
			// the edge represents a true join predicate between the two components
			!pbsFst->IsDisjoint(pedge->m_pbs) &&
			!pbsSnd->IsDisjoint(pedge->m_pbs))
		{
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif	// GPOS_DEBUG
				pbsEdges->ExchangeSet(ul);
			GPOS_ASSERT(!fSet);
		}
	}
	pbs->Release();

	CExpression *pexprPred = NULL;
	if (0 < pbsEdges->Size())
	{
		CExpressionArray *pdrgpexpr = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		CBitSetIter bsi(*pbsEdges);
		while (bsi.Advance())
		{
			ULONG ul = bsi.Bit();
			SEdge *pedge = m_rgpedge[ul];
			pedge->m_pexpr->AddRef();
			pdrgpexpr->Append(pedge->m_pexpr);
		}

		pexprPred = CPredicateUtils::PexprConjunction(m_mp, pdrgpexpr);
	}

	pbsEdges->Release();
	return pexprPred;
}

void
CJoinOrderDPv2::DeriveStats(CExpression *pexpr)
{
	try
	{
		// We want to let the histogram code compute the join selectivity and the number of NDVs based
		// on actual histogram buckets, taking into account the overlap of the data ranges. It helps
		// with getting more consistent and accurate cardinality estimates for DP.
		// Eventually, this should probably become the default method.
		CJoinStatsProcessor::SetComputeScaleFactorFromHistogramBuckets(true);
		CJoinOrder::DeriveStats(pexpr);
		CJoinStatsProcessor::SetComputeScaleFactorFromHistogramBuckets(false);
	}
	catch (...)
	{
		CJoinStatsProcessor::SetComputeScaleFactorFromHistogramBuckets(false);
		throw;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::GetJoinExprForProperties
//
//	@doc:
//		Build a CExpression joining the two given sets, choosing child
//		expressions with given properties
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::SExpressionInfo *
CJoinOrderDPv2::GetJoinExprForProperties(
	SGroupInfo *left_child, SGroupInfo *right_child,
	SExpressionProperties &required_properties)
{
	SGroupAndExpression left_expr =
		GetBestExprForProperties(left_child, required_properties);
	SGroupAndExpression right_expr =
		GetBestExprForProperties(right_child, required_properties);

	if (!left_expr.IsValid() || !right_expr.IsValid())
	{
		return NULL;
	}

	return GetJoinExpr(left_expr, right_expr, required_properties);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::GetJoinExpr
//
//	@doc:
//		Build a CExpression joining the two given sets from given expressions
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::SExpressionInfo *
CJoinOrderDPv2::GetJoinExpr(const SGroupAndExpression &left_child_expr,
							const SGroupAndExpression &right_child_expr,
							SExpressionProperties &result_properties)
{
	SGroupInfo *left_group_info = left_child_expr.m_group_info;

	if (IsRightChildOfNIJ(left_group_info))
	{
		// can't use the right child of an NIJ on the left side
		return NULL;
	}

	SExpressionInfo *left_expr_info = left_child_expr.GetExprInfo();
	SGroupInfo *right_group_info = right_child_expr.m_group_info;
	SExpressionInfo *right_expr_info = right_child_expr.GetExprInfo();

	CExpression *scalar_expr = NULL;
	CBitSet *required_on_left = NULL;
	BOOL isLOJ =
		IsRightChildOfNIJ(right_group_info, &scalar_expr, &required_on_left);

	if (!isLOJ)
	{
		// inner join, compute the predicate from the join graph
		GPOS_ASSERT(NULL == scalar_expr);
		scalar_expr = PexprBuildInnerJoinPred(left_group_info->m_atoms,
											  right_group_info->m_atoms);
	}
	else
	{
		// check whether scalar_expr can be computed from left_child and right_child,
		// otherwise this is not a valid join
		GPOS_ASSERT(NULL != scalar_expr && NULL != required_on_left);
		if (!left_group_info->m_atoms->ContainsAll(required_on_left))
		{
			// the left child does not produce all the values needed in the ON
			// predicate, so this is not a valid join
			return NULL;
		}
		scalar_expr->AddRef();
	}

	if (NULL == scalar_expr)
	{
		// this is a cross product

		if (right_group_info->IsAnAtom())
		{
			// generate a TRUE boolean expression as the join predicate of the cross product
			scalar_expr = CUtils::PexprScalarConstBool(m_mp, true);
		}
		else
		{
			// we don't do bushy cross products, any mandatory or optional cross products
			// are linear trees
			return NULL;
		}
	}

	CExpression *join_expr = NULL;

	CExpression *left_expr = left_expr_info->m_expr;
	CExpression *right_expr = right_expr_info->m_expr;
	left_expr->AddRef();
	right_expr->AddRef();

	if (isLOJ)
	{
		join_expr = CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(
			m_mp, left_expr, right_expr, scalar_expr);
	}
	else
	{
		join_expr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
			m_mp, left_expr, right_expr, scalar_expr);
	}

	return GPOS_NEW(m_mp) SExpressionInfo(m_mp, join_expr, left_child_expr,
										  right_child_expr, result_properties);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::IsASupersetOfProperties
//
//	@doc:
//		Return whether <prop> provides a superset of the properties <other_prop>
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDPv2::IsASupersetOfProperties(SExpressionProperties &prop,
										SExpressionProperties &other_prop)
{
	// are the bits in other_prop a subset of the bits in prop?
	return 0 == (other_prop.m_join_order & ~prop.m_join_order);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::ArePropertiesDisjoint
//
//	@doc:
//		Return whether each of the two properties provides something that
//		the other property doesn't provide.
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDPv2::ArePropertiesDisjoint(SExpressionProperties &prop,
									  SExpressionProperties &other_prop)
{
	return !IsASupersetOfProperties(prop, other_prop) &&
		   !IsASupersetOfProperties(other_prop, prop);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::GetBestExprForProperties
//
//	@doc:
//		Given a group and required properties, return an expression in the
//		group that satisfies the required properties or return an invalid
//		SGroupAndExpression object if no such expression exists.
//		Use SGroupAndExpression::IsValid() to test the validity of the
//		return value.
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::SGroupAndExpression
CJoinOrderDPv2::GetBestExprForProperties(SGroupInfo *group_info,
										 SExpressionProperties &props)
{
	ULONG best_ix = gpos::ulong_max;
	CDouble best_cost(0.0);

	for (ULONG ul = 0; ul < group_info->m_best_expr_info_array->Size(); ul++)
	{
		SExpressionInfo *expr_info = (*group_info->m_best_expr_info_array)[ul];

		if (IsASupersetOfProperties(expr_info->m_properties, props))
		{
			if (gpos::ulong_max == best_ix || expr_info->GetCost() < best_cost)
			{
				// we found a candidate with the best cost so far that satisfies the properties
				best_ix = ul;
				best_cost = expr_info->GetCost();
			}
		}
	}

	return SGroupAndExpression(group_info, best_ix);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddNewPropertyToExpr
//
//	@doc:
//		Add a new property that an existing expression in a group provides.
//		NOTE: This method should be used with care! Only add a property that
//		does not yet exist in the current level or in any higher level.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddNewPropertyToExpr(SExpressionInfo *expr_info,
									 SExpressionProperties props)
{
	expr_info->m_properties.Add(props);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddExprToGroupIfNecessary
//
//	@doc:
//		Check a new expression to see whether it provides any new property or
//		whether it provides the same property as an existing expression at a
//		lower cost. If so, add the new expression or replace an existing
//		expression with the better one. Otherwise, release the expression.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddExprToGroupIfNecessary(SGroupInfo *group_info,
										  SExpressionInfo *new_expr_info)
{
	// compute the cost for the new expression
	ComputeCost(new_expr_info, group_info->m_cardinality);
	CDouble new_cost = new_expr_info->GetCost();

	if (group_info->m_atoms->Size() == m_ulComps)
	{
		// At the top level, we have only one group. To be able to return multiple results
		// for the xform, we keep the top k expressions (all from the same group) in a KHeap
		new_expr_info->AddRef();
		if (new_expr_info->m_properties.Satisfies(EJoinOrderHasPS))
		{
			m_top_k_part_expressions->Insert(new_expr_info);
		}
		else
		{
			m_top_k_expressions->Insert(new_expr_info);
		}
	}

	if (0 == group_info->m_best_expr_info_array->Size() ||
		new_cost < group_info->m_lowest_expr_cost)
	{
		// update the low water mark for the cost seen in this group
		group_info->m_lowest_expr_cost = new_cost;
	}

	// loop through the existing expressions, comparing cost and properties with each
	// existing expression, and perform the following action if cost and properties of
	// the new expression are:
	//
	// case  properties  cost    action
	// ----  ----------  ------  -------------------
	//   1       <        <      continue
	//   2       <        >=     discard, stop
	//   3       =        <      replace, stop
	//   4       =        >=     discard, stop
	//   5       >        <=     replace, stop (*)
	//   6       >        >      continue
	//   7    different   any    continue
	//
	// if we reach the end of the list of existing expressions and have not yet stopped,
	// then we add the new expression.
	//
	// (*) Note that if we find a new expression that provides more properties for the same
	// or lower cost, we could potentially replace more than one expression. Right now this
	// is not done, we replace only the first such expression we find (see the rule below
	// for the reason).
	//
	// Since we are using indexes into the array of expressions, we follow this ground rule
	// to keep those indexes consistent: Once an SExpressionInfo is inserted into the
	// m_best_expr_info_array at an index i, this entry will remain at the same index.
	// This method ensures that its cost can only go down and its properties can only increase.
	// This rule holds across multiple enumeration algorithms. Therefore, SExpressionInfos from higher
	// groups can reliably refer to indexes in the m_best_expr_info_array of their child groups.

	BOOL discard = false;
	BOOL replaced_expr = false;

	for (ULONG ul = 0; ul < group_info->m_best_expr_info_array->Size(); ul++)
	{
		SExpressionInfo *expr_info = (*group_info->m_best_expr_info_array)[ul];
		BOOL old_ge_new = IsASupersetOfProperties(expr_info->m_properties,
												  new_expr_info->m_properties);
		BOOL new_ge_old = IsASupersetOfProperties(new_expr_info->m_properties,
												  expr_info->m_properties);
		CDouble old_cost = expr_info->GetCost();
		CDouble new_cost = new_expr_info->GetCost();

		if (old_ge_new)
		{
			if (!new_ge_old)
			{
				// new expression provides fewer properties
				if (new_cost < old_cost)
				{
					// case 1
					continue;
				}
				else
				{
					// case 2
					discard = true;
					break;
				}
			}
			else
			{
				// both expressions provide the same properties
				if (new_cost < old_cost)
				{
					// case 3
					group_info->m_best_expr_info_array->Replace(ul,
																new_expr_info);
					replaced_expr = true;
					break;
				}
				else
				{
					// case 4
					discard = true;
					break;
				}
			}
		}
		else
		{
			if (new_ge_old)
			{
				// new expression provides more properties
				if (new_cost <= old_cost)
				{
					// case 5
					group_info->m_best_expr_info_array->Replace(ul,
																new_expr_info);
					replaced_expr = true;
					break;
				}
				else
				{
					// case 6
					continue;
				}
			}
			else
			{
				// new expression provides different properties, neither more nor less
				// case 7
				continue;
			}
		}
	}

	if (discard)
	{
		// the new expression needs to be discarded
		new_expr_info->Release();
	}
	else if (!replaced_expr)
	{
		// we went through all existing expressions without replacing an existing one and
		// without deciding to discard the new expression, therefore we need to add the
		// new expression
		group_info->m_best_expr_info_array->Append(new_expr_info);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PopulateExpressionToEdgeMapIfNeeded
//
//	@doc:
//		In some cases we may not place all of the predicates in the NAry join in
//		the resulting tree of binary joins. If that situation is a possibility,
//		we'll create a map from expressions to edges, so that we can find any
//		unused edges to be placed in a select node on top of the join.
//
//		Examples:
//		select * from foo left join bar on foo.a=bar.a where coalesce(bar.b, 0) < 10;
//		select * from foo left join bar on foo.a=bar.a where foo.a = outer_ref;
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::PopulateExpressionToEdgeMapIfNeeded()
{
	BOOL populate = false;

	if (0 < m_outer_refs->Size())
	{
		// with outer refs we can get predicates like <col> = <outer ref>
		// that are not real join predicates
		populate = true;
	}

	if (!populate && NULL != m_child_pred_indexes)
	{
		// check for WHERE predicates involving LOJ right children

		// make a bitset b with all the LOJ right children
		CBitSet *loj_right_children = GPOS_NEW(m_mp) CBitSet(m_mp);

		for (ULONG c = 0; c < m_child_pred_indexes->Size(); c++)
		{
			if (0 < *((*m_child_pred_indexes)[c]))
			{
				loj_right_children->ExchangeSet(c);
			}
		}

		for (ULONG en1 = 0; en1 < m_ulEdges; en1++)
		{
			SEdge *pedge = m_rgpedge[en1];

			if (pedge->m_loj_num == 0)
			{
				// check whether this inner join (WHERE) predicate refers to any LOJ right child
				// (whether its bitset overlaps with b)
				// or whether we see any local predicates (this should be uncommon)
				if (!loj_right_children->IsDisjoint(pedge->m_pbs) ||
					1 == pedge->m_pbs->Size())
				{
					populate = true;
					break;
				}
			}
		}
		loj_right_children->Release();
	}

	if (populate)
	{
		m_expression_to_edge_map = GPOS_NEW(m_mp) ExpressionToEdgeMap(m_mp);

		for (ULONG en2 = 0; en2 < m_ulEdges; en2++)
		{
			SEdge *pedge = m_rgpedge[en2];

			pedge->AddRef();
			pedge->m_pexpr->AddRef();
			m_expression_to_edge_map->Insert(pedge->m_pexpr, pedge);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddSelectNodeForRemainingEdges
//
//	@doc:
//		add a select node with any remaining edges (predicates) that have
//		not been incorporated in the join tree
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::AddSelectNodeForRemainingEdges(CExpression *join_expr)
{
	if (NULL == m_expression_to_edge_map)
	{
		return join_expr;
	}

	CExpressionArray *exprArray = GPOS_NEW(m_mp) CExpressionArray(m_mp);
	RecursivelyMarkEdgesAsUsed(join_expr);

	// find any unused edges and add them to a select
	for (ULONG en = 0; en < m_ulEdges; en++)
	{
		SEdge *pedge = m_rgpedge[en];

		if (pedge->m_fUsed)
		{
			// mark the edge as unused for the next alternative, where
			// we will have to repeat this check
			pedge->m_fUsed = false;
		}
		else
		{
			// found an unused edge, this one will need to go into
			// a select node on top of the join
			pedge->m_pexpr->AddRef();
			exprArray->Append(pedge->m_pexpr);
		}
	}

	if (0 < exprArray->Size())
	{
		CExpression *conj = CPredicateUtils::PexprConjunction(m_mp, exprArray);

		return GPOS_NEW(m_mp) CExpression(
			m_mp, GPOS_NEW(m_mp) CLogicalSelect(m_mp), join_expr, conj);
	}

	exprArray->Release();

	return join_expr;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::RecursivelyMarkEdgesAsUsed
//
//	@doc:
//		mark all the edges corresponding to any part of <expr> as used
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::RecursivelyMarkEdgesAsUsed(CExpression *expr)
{
	GPOS_CHECK_STACK_SIZE;

	if (expr->Pop()->FLogical())
	{
		for (ULONG ul = 0; ul < expr->Arity(); ul++)
		{
			RecursivelyMarkEdgesAsUsed((*expr)[ul]);
		}
	}
	else
	{
		GPOS_ASSERT(expr->Pop()->FScalar());
		const SEdge *edge = m_expression_to_edge_map->Find(expr);
		if (NULL != edge)
		{
			// we found the edge belonging to this expression, terminate the recursion
			const_cast<SEdge *>(edge)->m_fUsed = true;
			return;
		}

		// we should not reach the leaves of the tree without finding an edge
		GPOS_ASSERT(0 < expr->Arity() || CUtils::FScalarConstTrue(expr));

		// this is an AND of multiple edges
		for (ULONG ul = 0; ul < expr->Arity(); ul++)
		{
			RecursivelyMarkEdgesAsUsed((*expr)[ul]);
		}
	}
}


// Consider partition selector/broadcast and populate join_expr_info accordingly
// Specifically, we're looking for joins in the form
//   Join
//     - DTS
//     - table scan (or tree)
//        - predicate
//
// In this case, we can put the PS over the TS
// We make a few assumptions:
//   1. The benefits of a PS are from the selectivity of a single table, rather than the join result between two tables. We find this table by looking for logical selects.
//   2. The selectivty of this single table is equal to the selectivity of the PS
//
// If the right atom is a PT, then we need to check if the left expression has a PS that may satisfy it.
// If it is, we mark this SExpressionInfo as containing a PS
// We only consider linear trees here, since bushy trees would increase the search space and increase the
// chance of motions between the PS and PT, which then would fail requirements during optimization
void
CJoinOrderDPv2::PopulateDPEInfo(SExpressionInfo *join_expr_info,
								SGroupInfo *part_table_group_info,
								SGroupInfo *part_selector_group_info)
{
	SGroupInfoArray *atom_groups = GetGroupsForLevel(1);

	CBitSetIter iter_pt(*part_table_group_info->m_atoms);
	SGroupInfo *pt_atom = NULL;
	CPartKeysArray *partition_keys = NULL;
	while (iter_pt.Advance())
	{
		pt_atom = (*atom_groups)[iter_pt.Bit()];
		partition_keys =
			(*pt_atom->m_best_expr_info_array)[0]->m_atom_part_keys_array;
		if (partition_keys != NULL && partition_keys->Size() > 0)
		{
			break;
		}
	}
	if (NULL != partition_keys)
	{
		GPOS_ASSERT(NULL != pt_atom);
		GPOS_ASSERT(NULL != partition_keys && partition_keys->Size() > 0);
		CExpression *join_expr = join_expr_info->m_expr;
		CExpression *scalar_expr = (*join_expr)[join_expr->Arity() - 1];

		CColRefSet *join_expr_cols = scalar_expr->DeriveUsedColumns();
		for (ULONG i = 0; i < partition_keys->Size(); i++)
		{
			CPartKeys *part_keys = (*partition_keys)[i];
			// If the join expr overlaps the partition key, then we consider the expression as having a possible PS for that PT
			if (part_keys->FOverlap(join_expr_cols))
			{
				CBitSetIter iter_ps(*part_selector_group_info->m_atoms);
				join_expr_info->m_contain_PS->ExchangeSet(iter_pt.Bit());
				while (iter_ps.Advance())
				{
					// if the part selector group has a potential PS, ensure that one of the group's atoms is a logical select
					if ((*(*atom_groups)[iter_ps.Bit()]
							  ->m_best_expr_info_array)[0]
							->m_expr->Pop()
							->Eopid() == COperator::EopLogicalSelect)
					{
						SExpressionInfo *atom_ps =
							(*(*atom_groups)[iter_ps.Bit()]
								  ->m_best_expr_info_array)[0];
						// This is a bit simplistic. We calculate how much we are reducing the cardinality of the atom, but also take into account the cost of broadcasting the inner rows. If the number of rows broadcasted is much larger than the savings, then PS will likely not benefit in this case
						// The numbers are from the cost model used during optimization

						// for a select(some_non_get_node()) ==> 0.9
						// for a non-select node (won't even come here) ==> 0.0, in effect
						// for a select(get) ==> 1 - (row count of select / row count of get)

						CDouble percent_reduction =
							.9;	 // an arbitary default if the logical operator is not a simple select
						ICostModel *cost_model =
							COptCtxt::PoctxtFromTLS()->GetCostModel();
						CDouble num_segments = cost_model->UlHosts();
						CDouble distribution_cost_factor =
							(num_segments * BCAST_RECV_COST + BCAST_SEND_COST) /
							SEQ_SCAN_COST;
						CDouble broadcast_penalty =
							part_selector_group_info->m_cardinality *
							distribution_cost_factor;

						if (atom_ps->m_atom_base_table_rows.Get() > 0)
						{
							percent_reduction =
								(1 - (atom_ps->m_cost.Get() /
									  atom_ps->m_atom_base_table_rows.Get()));
						}

						// Adjust the cost of the expression for each partition selector
						join_expr_info->m_cost_adj_PS =
							join_expr_info->m_cost_adj_PS -
							(percent_reduction * pt_atom->m_cardinality) +
							broadcast_penalty;
					}
				}
				break;
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::SearchJoinOrders
//
//	@doc:
//		Enumerate all the possible joins between two lists of components
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::SearchJoinOrders(ULONG left_level, ULONG right_level)
{
	GPOS_ASSERT(left_level > 0 && right_level > 0 &&
				left_level + right_level <= m_ulComps);

	SGroupInfoArray *left_group_info_array = GetGroupsForLevel(left_level);
	SGroupInfoArray *right_group_info_array = GetGroupsForLevel(right_level);
	SLevelInfo *current_level_info = Level(left_level + right_level);

	ULONG left_size = left_group_info_array->Size();
	ULONG right_size = right_group_info_array->Size();
	for (ULONG left_ix = 0; left_ix < left_size; left_ix++)
	{
		SGroupInfo *left_group_info = (*left_group_info_array)[left_ix];

		CBitSet *left_bitset = left_group_info->m_atoms;
		ULONG right_ix = 0;

		// if pairs from the same level, start from the next
		// entry to avoid duplicate join combinations
		// i.e a join b and b join a, just try one
		// commutativity will take care of the other
		// this assumption is also necessary to ensure
		// we generate correct results for LOJs
		if (left_level == right_level)
		{
			right_ix = left_ix + 1;
		}

		for (; right_ix < right_size; right_ix++)
		{
			SGroupInfo *right_group_info = (*right_group_info_array)[right_ix];
			CBitSet *right_bitset = right_group_info->m_atoms;

			if (!left_bitset->IsDisjoint(right_bitset))
			{
				// not a valid join, left and right tables must not overlap
				continue;
			}

			SExpressionProperties reqd_properties(EJoinOrderDP);
			SExpressionInfo *join_expr_info = GetJoinExprForProperties(
				left_group_info, right_group_info, reqd_properties);

			if (NULL != join_expr_info)
			{
				// we have a valid join

				CBitSet *join_bitset =
					GPOS_NEW(m_mp) CBitSet(m_mp, *left_bitset);

				join_bitset->Union(right_bitset);

				// Find the best expression for DP and add this to the group
				// This doesn't consider PS, but we still want to generate these alternatives
				SGroupInfo *group_info = LookupOrCreateGroupInfo(
					current_level_info, join_bitset, join_expr_info);
				AddExprToGroupIfNecessary(group_info, join_expr_info);

				// We only want to consider linear trees when enumerating partition selector alternatives
				if (right_level != 1)
				{
					continue;
				}

				// For PS alternatives, get the best join expression for any properties
				SExpressionProperties join_props(EJoinOrderAny);

				// Now search for new PS alternatives
				join_expr_info = GetJoinExprForProperties(
					left_group_info, right_group_info, join_props);



				// TODO: Reduce non-mandatory cross products

				PopulateDPEInfo(join_expr_info, left_group_info,
								right_group_info);
				// For the first level, we should consider joining both ways
				if (left_level == 1 && right_level == 1)
				{
					PopulateDPEInfo(join_expr_info, right_group_info,
									left_group_info);
				}

				if (join_expr_info->m_contain_PS->Size() > 0)
				{
					AddNewPropertyToExpr(
						join_expr_info, SExpressionProperties(EJoinOrderHasPS));
					AddExprToGroupIfNecessary(group_info, join_expr_info);
				}
				else
				{
					join_expr_info->Release();
				}
			}
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::GreedySearchJoinOrders
//
//	@doc:
//		Enumerate all the possible joins between a list of groups and the
//		list of atoms, only add the best new expression. Note that this
//		method is used for Query, Mincard and GreedyAvoidXProd join orders
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::GreedySearchJoinOrders(ULONG left_level, JoinOrderPropType algo)
{
	ULONG right_level = 1;
	GPOS_ASSERT(left_level > 0 && left_level + right_level <= m_ulComps);

	SGroupInfoArray *left_group_info_array = GetGroupsForLevel(left_level);
	SGroupInfoArray *right_group_info_array = GetGroupsForLevel(right_level);
	SLevelInfo *current_level_info = Level(left_level + right_level);
	SExpressionProperties left_reqd_properties(algo);
	SExpressionProperties right_reqd_properties(EJoinOrderAny);
	SExpressionProperties result_properties(algo);

	ULONG left_size = left_group_info_array->Size();
	ULONG right_size = right_group_info_array->Size();

	// pre-existing greedy solution on level left_level
	CBitSet *left_bitset = NULL;
	SGroupAndExpression left_child_expr_info;

	ULONG left_ix = 0;
	ULONG right_ix = 0;

	// the solution on level left_level+1 that we want to build
	SGroupInfo *best_group_info_in_level = NULL;
	SExpressionInfo *best_expr_info_in_level = NULL;
	CDouble best_cost_in_level(-1.0);

	// find the solution for the left side
	while (left_ix < left_size)
	{
		left_child_expr_info = GetBestExprForProperties(
			(*left_group_info_array)[left_ix], left_reqd_properties);

		if (left_child_expr_info.IsValid())
		{
			left_bitset = left_child_expr_info.m_group_info->m_atoms;
			// we found the one solution from the lower level that we will build upon
			break;
		}
		left_ix++;
	}

	if (left_ix >= left_size)
	{
		// we didn't find a greedy solution for the left side
		GPOS_ASSERT(!"No greedy solution found for the left side");
		return;
	}

	if (EJoinOrderQuery == algo)
	{
		// for query, we want to pick the atoms in sequence, indexes 0 ... n-1
		right_ix = left_level;
	}

	// now loop over all the atoms on the right and pick the one we want to use for this level
	for (; right_ix < right_size; right_ix++)
	{
		SGroupInfo *right_group_info = (*right_group_info_array)[right_ix];
		CBitSet *right_bitset = right_group_info->m_atoms;

		if (!left_bitset->IsDisjoint(right_bitset))
		{
			// not a valid join, left and right tables must not overlap
			continue;
		}

		SGroupAndExpression right_child_expr_info =
			GetBestExprForProperties(right_group_info, right_reqd_properties);

		if (!right_child_expr_info.IsValid())
		{
			continue;
		}

		SExpressionInfo *join_expr_info = GetJoinExpr(
			left_child_expr_info, right_child_expr_info, result_properties);
		if (NULL != join_expr_info)
		{
			// we have a valid join
			CBitSet *join_bitset = GPOS_NEW(m_mp) CBitSet(m_mp, *left_bitset);

			join_bitset->Union(right_bitset);

			// look up existing group and stats or create a new group and derive stats
			SGroupInfo *join_group_info = LookupOrCreateGroupInfo(
				current_level_info, join_bitset, join_expr_info);

			ComputeCost(join_expr_info, join_group_info->m_cardinality);
			CDouble join_cost = join_expr_info->GetCost();

			if (NULL == best_expr_info_in_level ||
				join_cost < best_cost_in_level)
			{
				best_group_info_in_level = join_group_info;
				CRefCount::SafeRelease(best_expr_info_in_level);
				best_expr_info_in_level = join_expr_info;
				best_cost_in_level = join_cost;
			}
			else
			{
				join_expr_info->Release();
			}

			if (EJoinOrderQuery == algo)
			{
				// we are done, we try only a single right index for join order query
				break;
			}
		}
	}

	if (NULL != best_expr_info_in_level)
	{
		// add the best expression from the loop with the specified properties
		// also add it to top k if we are at the top
		best_expr_info_in_level->m_properties.Add(algo);
		AddExprToGroupIfNecessary(best_group_info_in_level,
								  best_expr_info_in_level);
	}
	else
	{
		// we should always find a greedy solution
		GPOS_ASSERT(!"We should always find a greedy solution");
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::LookupOrCreateGroupInfo
//
//	@doc:
//		Look up a group from a given set of atoms. If found, return it.
//		If not found, create a new group in the specified level.
//		Note that this method consumes a RefCount on <atoms> but it does
//		not consume refcounts from <levelInfo> or <stats_expr_info>.
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::SGroupInfo *
CJoinOrderDPv2::LookupOrCreateGroupInfo(SLevelInfo *levelInfo, CBitSet *atoms,
										SExpressionInfo *stats_expr_info)
{
	SGroupInfo *group_info = m_bitset_to_group_info_map->Find(atoms);
	SExpressionInfo *real_expr_info_for_stats = stats_expr_info;

	if (NULL == group_info)
	{
		// this is a group we haven't seen yet, create a new group info and derive stats, if needed
		group_info = GPOS_NEW(m_mp) SGroupInfo(m_mp, atoms);
		if (!stats_expr_info->m_properties.Satisfies(EJoinOrderStats))
		{
			SExpressionProperties stats_props(EJoinOrderStats);

			// need to derive stats, make sure we use an expression whose children already have stats
			real_expr_info_for_stats = GetJoinExprForProperties(
				stats_expr_info->m_left_child_expr.m_group_info,
				stats_expr_info->m_right_child_expr.m_group_info, stats_props);

			DeriveStats(real_expr_info_for_stats->m_expr);
		}
		else
		{
			GPOS_ASSERT(NULL != real_expr_info_for_stats->m_expr->Pstats());
			// we are using stats_expr_info in the new group, but the caller didn't
			// allocate a ref count for us, so add one here
			stats_expr_info->AddRef();
		}
		group_info->m_cardinality =
			real_expr_info_for_stats->m_expr->Pstats()->Rows();
		AddExprToGroupIfNecessary(group_info, real_expr_info_for_stats);

		if (NULL == levelInfo->m_top_k_groups)
		{
			// no limits, just add the group to the array
			// note that the groups won't be sorted by cost in this case
			levelInfo->m_groups->Append(group_info);
		}
		else
		{
			// insert into the KHeap for now, the best groups will be transferred to
			// levelInfo->m_groups when we call FinalizeDPLevel()
			levelInfo->m_top_k_groups->Insert(group_info);
		}

		if (1 < levelInfo->m_level)
		{
			// also insert into the bitset to group map
			group_info->m_atoms->AddRef();
			group_info->AddRef();
			m_bitset_to_group_info_map->Insert(group_info->m_atoms, group_info);
		}
	}
	else
	{
		atoms->Release();
	}

	return group_info;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::FinalizeDPLevel
//
//	@doc:
//		Called when we finish a level in the DP enumeration algorithm. Apply
//		limit on the number of groups and move the remaining groups into the
//		SLevelInfo::m_groups array.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::FinalizeDPLevel(ULONG level)
{
	GPOS_ASSERT(level >= 2);
	SLevelInfo *level_info = Level(level);

	if (NULL != level_info->m_top_k_groups)
	{
		SGroupInfo *winner;

		while (NULL !=
			   (winner = level_info->m_top_k_groups->RemoveBestElement()))
		{
			// add the next best group to the level array, sorted by ascending cost
			level_info->m_groups->Append(winner);
		}

		SGroupInfo *loser;

		// also remove the groups that didn't make it from the bitset to group info map
		while (NULL !=
			   (loser = level_info->m_top_k_groups->RemoveNextElement()))
		{
			m_bitset_to_group_info_map->Delete(loser->m_atoms);
			loser->Release();
		}

		// release the remaining groups at this time, they won't be needed anymore
		level_info->m_top_k_groups->Release();
		level_info->m_top_k_groups = NULL;
	}
}



//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::SearchBushyJoinOrders
//
//	@doc:
//		Generate all bushy join trees of level current_level
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::SearchBushyJoinOrders(ULONG current_level)
{
	if (LevelIsFull(current_level))
	{
		// we've exceeded the number of joins for which we generate bushy trees
		// TODO: Transition off of bushy joins more gracefully, note that bushy
		// trees usually do't add any more groups, they just generate more
		// expressions for existing groups
		return;
	}

	// Try bushy joins of bitsets of level x and y, where
	// x + y = current_level and x > 1 and y > 1 and x >= y.
	// Note that join trees of level 3 and below are never bushy,
	// so this loop only executes at current_level >= 4
	for (ULONG right_level = 2; right_level <= current_level / 2; right_level++)
	{
		SearchJoinOrders(current_level - right_level, right_level);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PExprExpand
//
//	@doc:
//		Main driver for join order enumeration, called by xform
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::PexprExpand()
{
	// put the "atoms", the nodes of the join tree that
	// are not joins themselves, at the first level
	SLevelInfo *atom_level = Level(1);

	// the atoms all have stats derived
	SExpressionProperties atom_props(EJoinOrderStats + EJoinOrderDP);

	// populate level 1 with the atoms (the logical children of the NAry join)
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	for (ULONG atom_id = 0; atom_id < m_ulComps; atom_id++)
	{
		CBitSet *atom_bitset = GPOS_NEW(m_mp) CBitSet(m_mp);
		atom_bitset->ExchangeSet(atom_id);
		CExpression *pexpr_atom = m_rgpcomp[atom_id]->m_pexpr;
		pexpr_atom->AddRef();
		SExpressionInfo *atom_expr_info =
			GPOS_NEW(m_mp) SExpressionInfo(m_mp, pexpr_atom, atom_props);
		if (0 == atom_id)
		{
			// this is the level 1 solution for the query join order
			atom_expr_info->m_properties.Add(EJoinOrderQuery);
		}

		// populate partition keys for atoms. This is an array of partition keys for each atom, NULL for non-partition tables
		CPartInfo *part_info = pexpr_atom->DerivePartitionInfo();
		if (part_info->UlConsumers() > 0)
		{
			atom_expr_info->m_atom_part_keys_array =
				part_info->Pdrgppartkeys(0);
			// mark this atom as containing a PS
			AddNewPropertyToExpr(atom_expr_info,
								 SExpressionProperties(EJoinOrderHasPS));
		}

		// Get base table descriptor if possible. We're particularly interested in Logical Gets/Selects
		// We need the underlying partition and table row information to properly estimate cardinality for
		// partition selection. If this is a logical expr that is more complex (eg: cte, nary join), we
		// will use a default estimate
		CTableDescriptor *table_desc = pexpr_atom->DeriveTableDescriptor();

		if (table_desc != NULL)
		{
			IMDId *rel_mdid = table_desc->MDId();
			rel_mdid->AddRef();
			CMDIdRelStats *rel_stats_mdid =
				GPOS_NEW(m_mp) CMDIdRelStats(CMDIdGPDB::CastMdid(rel_mdid));
			const IMDRelStats *pmdRelStats =
				md_accessor->Pmdrelstats(rel_stats_mdid);
			rel_stats_mdid->Release();

			atom_expr_info->m_atom_base_table_rows =
				std::max(DOUBLE(1.0), pmdRelStats->Rows().Get());
		}

		LookupOrCreateGroupInfo(atom_level, atom_bitset, atom_expr_info);

		// note that for atoms with stats, the above call will also insert atom_expr_info as first (and only)
		// expression into the group

		atom_expr_info->Release();
	}

	// call all the enumeration strategies, start with DP, as it builds some needed data structures
	// for MinCard and GreedyAvoidXProd
	EnumerateDP();
	EnumerateQuery();
	EnumerateMinCard();
	EnumerateGreedyAvoidXProd();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::EnumerateDP
//
//	@doc:
//		Exhaustive enumeration of join orders, try linear and bushy trees
//		and cross products. This method limits the join orders in two ways:
//		First, if it generates an expression A join B, then it won't also
//		generate B join A (this is left to the join commutativity rule).
//		Second, we may apply limits to the number of groups when we finalize
//		each level.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::EnumerateDP()
{
	if (GPOS_FTRACE(EopttraceGreedyOnlyInDPv2) ||
		GPOS_FTRACE(EopttraceMinCardOnlyInDPv2) ||
		GPOS_FTRACE(EopttraceQueryOnlyInDPv2))
	{
		return;
	}

	COptimizerConfig *optimizer_config =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	const CHint *phint = optimizer_config->GetHint();
	ULONG join_order_exhaustive_limit = phint->UlJoinOrderDPLimit();

	// for larger joins, compute the limit for the number of groups at each level, this
	// follows the number of groups for the largest join for which we do exhaustive search
	if (join_order_exhaustive_limit < m_ulComps)
	{
		for (ULONG l = 2; l <= m_ulComps; l++)
		{
			ULONG number_of_allowed_groups = 0;

			if (l < join_order_exhaustive_limit)
			{
				// at lower levels, limit the number of groups to that of an
				// <join_order_exhaustive_limit>-way join
				number_of_allowed_groups =
					NChooseK(join_order_exhaustive_limit, l);
			}
			else
			{
				// beyond that, use greedy (keep only one group per level)
				number_of_allowed_groups = 1;
			}

			// add a KHeap to this level, so that we can collect the k best expressions
			// while we are building the level
			Level(l)->m_top_k_groups =
				GPOS_NEW(m_mp) KHeap<SGroupInfoArray, SGroupInfo>(
					m_mp, number_of_allowed_groups);
		}
	}


	// build n-ary joins from the bottom up, starting with 2-way, 3-way up to m_ulComps-way
	for (ULONG current_join_level = 2; current_join_level <= m_ulComps;
		 current_join_level++)
	{
		// build linear joins, with a "current_join_level-1"-way join on one
		// side and an atom on the other side
		SearchJoinOrders(current_join_level - 1, 1);

		// build bushy trees - joins between two other joins
		SearchBushyJoinOrders(current_join_level);

		// finalize level, enforce limit for groups
		FinalizeDPLevel(current_join_level);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::EnumerateQuery
//
//	@doc:
//		Generate a tree that has the same join order as the SQL query. Note
//		that the generated tree is linear, even if the SQL query was bushy.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::EnumerateQuery()
{
	if (GPOS_FTRACE(EopttraceGreedyOnlyInDPv2) ||
		GPOS_FTRACE(EopttraceMinCardOnlyInDPv2))
	{
		return;
	}

	for (ULONG current_join_level = 2; current_join_level <= m_ulComps;
		 current_join_level++)
	{
		GreedySearchJoinOrders(current_join_level - 1, EJoinOrderQuery);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::FindLowestCardTwoWayJoin
//
//	@doc:
//		Find the 2-way join with the smallest cardinality. This is the
//		starting base for MinCard and GreedyAvoidXProd - these algorithms
//		don't start with the smallest atom.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::FindLowestCardTwoWayJoin(JoinOrderPropType prop_type)
{
	if (GPOS_FTRACE(EopttraceQueryOnlyInDPv2))
	{
		return;
	}

	if (GPOS_FTRACE(EopttraceGreedyOnlyInDPv2) ||
		GPOS_FTRACE(EopttraceMinCardOnlyInDPv2))
	{
		// due to above traceflags being turned on, EnumerateDP() didn't create
		// the necessary two way join orders. We have to create them here.
		SearchJoinOrders(1, 1);
	}

	SLevelInfo *level_2 = Level(2);
	CDouble min_card(0.0);
	SGroupInfo *min_card_group = NULL;
	SExpressionProperties any_props(EJoinOrderAny);

	// loop over all the 2-way joins and find the one with the lowest cardinality
	for (ULONG ul = 0; ul < level_2->m_groups->Size(); ul++)
	{
		SGroupInfo *group_2 = (*level_2->m_groups)[ul];
		CDouble group_2_cardinality = group_2->m_cardinality;
		CExpression *first_expr = (*group_2->m_best_expr_info_array)[0]->m_expr;
		if (EJoinOrderGreedyAvoidXProd == prop_type &&
			CUtils::FCrossJoin(first_expr))
		{
			group_2_cardinality = group_2_cardinality * m_cross_prod_penalty;
		}
		if (NULL == min_card_group || group_2_cardinality < min_card)
		{
			min_card = group_2_cardinality;
			min_card_group = group_2;
		}
	}

	// mark the lowest cardinality 2-way join as the MinCard and GreedyAvoidXProd solutions
	SGroupAndExpression min_card_2_way_join =
		GetBestExprForProperties(min_card_group, any_props);

	AddNewPropertyToExpr(min_card_2_way_join.GetExprInfo(),
						 SExpressionProperties(prop_type));
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::EnumerateMinCard
//
//	@doc:
//		Create a linear join tree, using the MinCard algorithm. We create
//		level n+1 of the tree by combining the MinCard solution of level
//		n (n>2) with the atom that produces the lowest cardinality join.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::EnumerateMinCard()
{
	if (GPOS_FTRACE(EopttraceQueryOnlyInDPv2) ||
		GPOS_FTRACE(EopttraceGreedyOnlyInDPv2))
	{
		return;
	}

	FindLowestCardTwoWayJoin(EJoinOrderMincard);
	for (ULONG current_join_level = 3; current_join_level <= m_ulComps;
		 current_join_level++)
	{
		GreedySearchJoinOrders(current_join_level - 1, EJoinOrderMincard);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::EnumerateGreedyAvoidXProd
//
//	@doc:
//		Create a linear join tree, using the GreedyAvoidXProd algorithm. This
//		is similar to MinCard, except that we avoid unnecessary cross
//		products. Note that this corresponds to the "greedy" value in the
//		optimizer_join_order guc, but we call it GreedyAvoidXProd here, since
//		Query, MinCard and GreedyAvoidXProd are all greedy enumeration
//		algorithms.
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::EnumerateGreedyAvoidXProd()
{
	if (GPOS_FTRACE(EopttraceQueryOnlyInDPv2) ||
		GPOS_FTRACE(EopttraceMinCardOnlyInDPv2))
	{
		return;
	}

	// avoid cross products by adding a very high penalty to their cost
	// note that we can still do mandatory cross products
	CDouble original_cross_prod_penalty = m_cross_prod_penalty;
	m_cross_prod_penalty = GPOPT_DPV2_CROSS_JOIN_GREEDY_PENALTY;

	FindLowestCardTwoWayJoin(EJoinOrderGreedyAvoidXProd);
	for (ULONG current_join_level = 3; current_join_level <= m_ulComps;
		 current_join_level++)
	{
		GreedySearchJoinOrders(current_join_level - 1,
							   EJoinOrderGreedyAvoidXProd);
	}
	m_cross_prod_penalty = original_cross_prod_penalty;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::GetNextOfTopK
//
//	@doc:
//		Return the next of the stored expressions in the top level. This
//		expression can then be used as an alternative of the transform.
//		Return NULL if there are no more alternatives.
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::GetNextOfTopK()
{
	SExpressionInfo *join_result_info =
		m_top_k_expressions->RemoveBestElement();
	if (NULL == join_result_info)
	{
		join_result_info = m_top_k_part_expressions->RemoveBestElement();
	}

	if (NULL == join_result_info)
	{
		return NULL;
	}

	CExpression *join_result = join_result_info->m_expr;

	join_result->AddRef();
	join_result_info->Release();

	return AddSelectNodeForRemainingEdges(join_result);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::IsRightChildOfNIJ
//
//	@doc:
//		Given a group info that is an atom, is this group the right child of
//		a non-inner-join (like an LOJ)?
//		Return false, if the group is not an atom. If the method returns
//		true, then it also returns the ON predicate to use for the join
//		and the set of atoms that have to be present in the left side of
//		the join (the join's right side will be <groupInfo>).
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDPv2::IsRightChildOfNIJ(SGroupInfo *groupInfo,
								  CExpression **onPredToUse,
								  CBitSet **requiredBitsOnLeft)
{
	GPOS_ASSERT(NULL == onPredToUse || NULL == *onPredToUse);
	GPOS_ASSERT(NULL == requiredBitsOnLeft || NULL == *requiredBitsOnLeft);

	if (1 != groupInfo->m_atoms->Size() || 0 == m_on_pred_conjuncts->Size())
	{
		// this is not a non-join vertex component (and only those can be right
		// children of NIJs), or the entire NAry join doesn't contain any NIJs
		return false;
	}

	// get the child predicate index for the non-join vertex component represented
	// by this component
	CBitSetIter iter(*groupInfo->m_atoms);

	// there is only one bit set for this component
	iter.Advance();

	ULONG childPredIndex = *(*m_child_pred_indexes)[iter.Bit()];

	if (GPOPT_ZERO_INNER_JOIN_PRED_INDEX != childPredIndex)
	{
		// this non-join vertex component is the right child of an
		// NIJ, return the ON predicate to use (if requested) and also return TRUE
		if (NULL != onPredToUse)
		{
			*onPredToUse = (*m_on_pred_conjuncts)[childPredIndex - 1];
		}
		if (NULL != requiredBitsOnLeft)
		{
			// also return the required minimal component on the left side of the join
			*requiredBitsOnLeft =
				(*m_non_inner_join_dependencies)[childPredIndex - 1];
		}
		return true;
	}

	// this is a non-join vertex component that is not the right child of an NIJ
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::FindLogicalChildByNijId
//
//	@doc:
//		Which of the NAry join children is the n-th NIJ child?
//		Return 0 if no such child exists.
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDPv2::FindLogicalChildByNijId(ULONG nij_num)
{
	GPOS_ASSERT(NULL != m_child_pred_indexes);

	for (ULONG c = 0; c < m_child_pred_indexes->Size(); c++)
	{
		if (*(*m_child_pred_indexes)[c] == nij_num)
		{
			return c;
		}
	}

	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::NChooseK
//
//	@doc:
//		Static method to calculate the mathematical (n choose k) formula:
//		n! / (k! * (n-k)!)
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDPv2::NChooseK(ULONG n, ULONG k)
{
	ULLONG numerator = 1;
	ULLONG denominator = 1;

	for (ULONG i = 1; i <= k; i++)
	{
		numerator *= n + 1 - i;
		denominator *= i;
	}

	return (ULONG)(numerator / denominator);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::LevelIsFull
//
//	@doc:
//		Return whether a given level has a limit for the number of groups in
//		it and whether that limit has already been exceeded
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDPv2::LevelIsFull(ULONG level)
{
	SLevelInfo *li = Level(level);

	if (NULL == li->m_top_k_groups)
	{
		return false;
	}

	return li->m_top_k_groups->IsLimitExceeded();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDPv2::OsPrint(IOstream &os) const
{
	// increase GPOS_LOG_MESSAGE_BUFFER_SIZE in file ILogger.h if the output of this method gets truncated
	ULONG num_levels = m_join_levels->Size();
	ULONG num_bitsets = 0;
	CPrintPrefix pref(NULL, "      ");

	for (ULONG lev = 1; lev < num_levels; lev++)
	{
		SGroupInfoArray *bitsets_this_level = GetGroupsForLevel(lev);
		ULONG num_bitsets_this_level = bitsets_this_level->Size();

		os << "CJoinOrderDPv2 - Level: " << lev << " ("
		   << bitsets_this_level->Size() << " group(s))" << std::endl;

		for (ULONG c = 0; c < num_bitsets_this_level; c++)
		{
			SGroupInfo *gi = (*bitsets_this_level)[c];
			ULONG num_exprs = gi->m_best_expr_info_array->Size();
			SExpressionProperties stats_properties(EJoinOrderStats);
			SGroupAndExpression expr_for_stats =
				const_cast<CJoinOrderDPv2 *>(this)->GetBestExprForProperties(
					gi, stats_properties);

			num_bitsets++;
			os << "   Group: ";
			gi->m_atoms->OsPrint(os);
			os << std::endl;

			if (expr_for_stats.IsValid())
			{
				os << "   Rows: "
				   << expr_for_stats.GetExprInfo()->m_expr->Pstats()->Rows()
				   << std::endl;
				// uncomment this for more detailed debugging
				// os << "   Expr for stats:" << std::endl;
				// expr_for_stats->OsPrint(os, &pref);
				// os << std::endl;
			}

			for (ULONG x = 0; x < num_exprs; x++)
			{
				SExpressionInfo *expr_info = (*gi->m_best_expr_info_array)[x];

				os << "   Expression with properties ";
				OsPrintProperty(os, expr_info->m_properties);
				os << "   m_contain_ps bitset: ";
				expr_info->m_contain_PS->OsPrint(os);
				os << std::endl;

				if (!gi->IsAnAtom())
				{
					os << "   Child groups: ";
					expr_info->m_left_child_expr.m_group_info->m_atoms->OsPrint(
						os);
					if (COperator::EopLogicalLeftOuterJoin ==
						expr_info->m_expr->Pop()->Eopid())
					{
						os << " left";
					}
					os << " join ";
					expr_info->m_right_child_expr.m_group_info->m_atoms
						->OsPrint(os);
					os << std::endl;
					os << "   left child cost: "
					   << expr_info->m_left_child_expr.m_group_info
							  ->m_lowest_expr_cost
					   << std::endl;
					os << "   right child cost: "
					   << expr_info->m_right_child_expr.m_group_info
							  ->m_lowest_expr_cost
					   << std::endl;
				}
				os << "   Cost: ";
				expr_info->m_cost.OsPrint(os);
				os << "   Partition Selector Penalty Cost: ";
				expr_info->m_cost_adj_PS.OsPrint(os);
				os << "   Total Cost with Partition Selector Penalty: ";
				expr_info->GetCost().OsPrint(os);
				os << std::endl;

				if (lev == 1)
				{
					os << "   Atom: " << std::endl;
					expr_info->m_expr->OsPrintExpression(os, &pref);
				}
				else if (lev < num_levels - 1)
				{
					os << "   Join predicate: " << std::endl;
					(*expr_info->m_expr)[2]->OsPrintExpression(os, &pref);
				}
				else
				{
					os << "   Top-level expression: " << std::endl;
					expr_info->m_expr->OsPrintExpression(os, &pref);
				}

				os << std::endl;
			}
		}
	}

	os << "CJoinOrderDPv2 - total number of groups: " << num_bitsets
	   << std::endl;

	return os;
}


IOstream &
CJoinOrderDPv2::OsPrintProperty(IOstream &os,
								SExpressionProperties &props) const
{
	os << "{ ";
	if (0 == props.m_join_order)
	{
		os << "";
	}
	else
	{
		BOOL is_first = true;

		if (props.Satisfies(EJoinOrderQuery))
		{
			os << "Query";
			is_first = false;
		}
		if (props.Satisfies(EJoinOrderMincard))
		{
			if (!is_first)
				os << ", ";
			os << "Mincard";
			is_first = false;
		}
		if (props.Satisfies(EJoinOrderGreedyAvoidXProd))
		{
			if (!is_first)
				os << ", ";
			os << "GreedyAvoidXProd";
			is_first = false;
		}
		if (props.Satisfies(EJoinOrderHasPS))
		{
			if (!is_first)
				os << ", ";
			os << "HasPS";
		}
		if (props.Satisfies(EJoinOrderStats))
		{
			if (!is_first)
				os << ", ";
			os << "Stats";
		}
		if (props.Satisfies(EJoinOrderDP))
		{
			if (!is_first)
				os << ", ";
			os << "DP";
		}
	}
	os << " }";

	return os;
}


#ifdef GPOS_DEBUG
void
CJoinOrderDPv2::DbgPrint()
{
	CAutoTrace at(m_mp);

	OsPrint(at.Os());
}
#endif


// EOF
