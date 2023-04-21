//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJoinOrder.cpp
//
//	@doc:
//		Implementation of join order logic
//---------------------------------------------------------------------------

#include "gpopt/xforms/CJoinOrder.h"

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		ICmpEdgesByLength
//
//	@doc:
//		Comparison function for simple join ordering: sort edges by length
//		only to guaranteed that single-table predicates don't end up above
//		joins;
//
//---------------------------------------------------------------------------
INT
ICmpEdgesByLength(const void *pvOne, const void *pvTwo)
{
	CJoinOrder::SEdge *pedgeOne = *(CJoinOrder::SEdge **) pvOne;
	CJoinOrder::SEdge *pedgeTwo = *(CJoinOrder::SEdge **) pvTwo;


	INT iDiff = (pedgeOne->m_pbs->Size() - pedgeTwo->m_pbs->Size());
	if (0 == iDiff)
	{
		return (INT) pedgeOne->m_pbs->HashValue() -
			   (INT) pedgeTwo->m_pbs->HashValue();
	}

	return iDiff;
}


// ctor
CJoinOrder::SComponent::SComponent(CMemoryPool *mp, CExpression *pexpr,
								   INT parent_loj_id, EPosition position)
	: m_pbs(NULL),
	  m_edge_set(NULL),
	  m_pexpr(pexpr),
	  m_fUsed(false),
	  m_parent_loj_id(parent_loj_id),
	  m_position(position)
{
	m_pbs = GPOS_NEW(mp) CBitSet(mp);
	m_edge_set = GPOS_NEW(mp) CBitSet(mp);
	GPOS_ASSERT_IMP(EpSentinel != m_position,
					NON_LOJ_DEFAULT_ID < m_parent_loj_id);
}

// ctor
CJoinOrder::SComponent::SComponent(CExpression *pexpr, CBitSet *pbs,
								   CBitSet *edge_set, INT parent_loj_id,
								   EPosition position)
	: m_pbs(pbs),
	  m_edge_set(edge_set),
	  m_pexpr(pexpr),
	  m_fUsed(false),
	  m_parent_loj_id(parent_loj_id),
	  m_position(position)
{
	GPOS_ASSERT(NULL != pbs);
	GPOS_ASSERT_IMP(EpSentinel != m_position,
					NON_LOJ_DEFAULT_ID < m_parent_loj_id);
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::~SComponent
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent::~SComponent()
{
	m_pbs->Release();
	m_edge_set->Release();
	CRefCount::SafeRelease(m_pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::SComponent::OsPrint(IOstream &os) const
{
	CBitSet *pbs = m_pbs;
	os << "Component: ";
	os << (*pbs) << std::endl;
	os << *m_pexpr << std::endl;

	if (m_parent_loj_id > NON_LOJ_DEFAULT_ID)
	{
		GPOS_ASSERT(m_position != EpSentinel);
		os << "Parent LOJ id: ";
		os << m_parent_loj_id << std::endl;
		os << "Child Position: ";
		os << m_position << std::endl;
	}

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::SEdge
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SEdge::SEdge(CMemoryPool *mp, CExpression *pexpr, ULONG loj_num)
	: m_pbs(NULL), m_pexpr(pexpr), m_loj_num(loj_num), m_fUsed(false)
{
	m_pbs = GPOS_NEW(mp) CBitSet(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::~SEdge
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SEdge::~SEdge()
{
	m_pbs->Release();
	m_pexpr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::SEdge::OsPrint(IOstream &os) const
{
	return os << (m_loj_num > 0 ? "Edge (loj): " : "Edge : ") << (*m_pbs)
			  << std::endl
			  << (*m_pexpr) << std::endl;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::CJoinOrder
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::CJoinOrder(CMemoryPool *mp, CExpressionArray *all_components,
					   CExpressionArray *inner_join_conjuncts,
					   BOOL include_loj_childs)
	: m_mp(mp),
	  m_rgpedge(NULL),
	  m_ulEdges(0),
	  m_rgpcomp(NULL),
	  m_ulComps(0),
	  m_include_loj_childs(include_loj_childs)
{
	typedef SComponent *Pcomp;
	typedef SEdge *Pedge;

	const ULONG num_of_nary_children = all_components->Size();
	INT num_of_lojs = 0;

	// Since we are using a static array, we need to know size of the array before hand
	// e.g.
	// +--CLogicalNAryJoin
	// |--CLogicalGet "t1"
	// |--CLogicalLeftOuterJoin
	// |  |--CLogicalGet "t5"
	// |  |--CLogicalGet "t4"
	// |  +--CScalarCmp (=)
	// +--CScalarCmp (=)
	//
	// In above case the pdrgpexpr comes with two elements in it:
	//  - CLogicalGet "t1"
	//  - CLogicalLeftOuterJoin
	// We need to create components out of "t1", "t4", "t5" and store them
	// in m_rgcomp.

	if (m_include_loj_childs)
	{
		for (ULONG ul = 0; ul < num_of_nary_children; ul++)
		{
			CExpression *pexpr = (*all_components)[ul];
			if (COperator::EopLogicalLeftOuterJoin == pexpr->Pop()->Eopid())
			{
				num_of_lojs++;
			}
		}
	}

	// total num of components = all_components + no of LOJs
	// since each LOJ is replaced with two components: its left & right children
	m_ulComps = num_of_nary_children + num_of_lojs;
	m_rgpcomp = GPOS_NEW_ARRAY(mp, Pcomp, m_ulComps);

	m_ulEdges = inner_join_conjuncts->Size() + num_of_lojs;
	m_rgpedge = GPOS_NEW_ARRAY(mp, Pedge, m_ulEdges);

	INT loj_id = 0;
	INT comp_num = 0;

	// add the inner join edges first
	for (ULONG ul = 0; ul < inner_join_conjuncts->Size(); ul++)
	{
		CExpression *pexprEdge = (*inner_join_conjuncts)[ul];
		pexprEdge->AddRef();
		m_rgpedge[ul] = GPOS_NEW(mp) SEdge(mp, pexprEdge, 0 /* not an LOJ */);
	}

	// process LOJs & add components & LOJ edges
	for (ULONG ul = 0; ul < num_of_nary_children; ul++)
	{
		CExpression *expr = (*all_components)[ul];
		if (m_include_loj_childs &&
			COperator::EopLogicalLeftOuterJoin == expr->Pop()->Eopid())
		{
			// counter for number of loj available in tree
			++loj_id;

			// add left child
			AddComponent(mp, (*expr)[0], loj_id, EpLeft, comp_num);

			// add right child.
			AddComponent(mp, (*expr)[1], loj_id, EpRight, comp_num + 1);

			// create the edge between the LOJ components right here - so as to
			// capture the relevant components from the LOJ predicate in the ON
			// clause; this is needed, later when assigning this edge to appropriate
			// LOJ, especially in the case that the predicate doesn't include colrefs
			// from both left & right components
			CExpression *scalar_expr = (*expr)[2];
			scalar_expr->AddRef();
			ULONG edge_idx = inner_join_conjuncts->Size() + loj_id - 1;
			m_rgpedge[edge_idx] =
				GPOS_NEW(mp) SEdge(mp, scalar_expr, 1 /* is an LOJ */);

			m_rgpcomp[comp_num]->m_edge_set->ExchangeSet(edge_idx);
			m_rgpedge[edge_idx]->m_pbs->ExchangeSet(comp_num);

			m_rgpcomp[comp_num + 1]->m_edge_set->ExchangeSet(edge_idx);
			m_rgpedge[edge_idx]->m_pbs->ExchangeSet(comp_num + 1);

			comp_num += 2;
		}
		else
		{
			AddComponent(mp, expr, NON_LOJ_DEFAULT_ID, EpSentinel, comp_num);
			comp_num += 1;
		}
	}

	ComputeEdgeCover();

	all_components->Release();
	inner_join_conjuncts->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::CJoinOrder
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::CJoinOrder(CMemoryPool *mp, CExpressionArray *all_components,
					   CExpressionArray *innerJoinPredConjuncts,
					   CExpressionArray *onPreds,
					   ULongPtrArray *childPredIndexes)
	: m_mp(mp),
	  m_rgpedge(NULL),
	  m_ulEdges(0),
	  m_rgpcomp(NULL),
	  m_ulComps(0),
	  m_include_loj_childs(false)  // not used by CXformExpandNAryJoinDPv2
{
	typedef SComponent *Pcomp;
	typedef SEdge *Pedge;

	const ULONG num_of_nary_children = all_components->Size();

	m_ulComps = all_components->Size();
	m_rgpcomp = GPOS_NEW_ARRAY(mp, Pcomp, m_ulComps);

	m_ulEdges = innerJoinPredConjuncts->Size() + onPreds->Size();
	m_rgpedge = GPOS_NEW_ARRAY(mp, Pedge, m_ulEdges);
	ULONG innerJoinEdges = innerJoinPredConjuncts->Size();

	// add the inner join edges first
	for (ULONG ul = 0; ul < innerJoinEdges; ul++)
	{
		CExpression *pexprEdge = (*innerJoinPredConjuncts)[ul];
		pexprEdge->AddRef();
		m_rgpedge[ul] = GPOS_NEW(mp) SEdge(mp, pexprEdge, 0 /* not an LOJ */);
	}

	// add the left outer join edges
	for (ULONG ul2 = 0; ul2 < onPreds->Size(); ul2++)
	{
		CExpression *pexprEdge = (*onPreds)[ul2];
		// a 1-based id of the LOJ in this NAry join
		ULONG lojId = ul2 + 1;
		// the logical child index (right LOJ child) that belongs to our current predicate pexprEdge
		ULONG logicalChildIndex = 0;

		// search for number lojId in the childPredIndexes array, the
		// entry that contains this number will be the logical child that
		// is associated with our ON predicate pexprEdge
		for (ULONG ci = 0; ci < childPredIndexes->Size(); ci++)
		{
			if (*(*childPredIndexes)[ci] == lojId)
			{
				logicalChildIndex = ci;
				break;
			}
		}

		GPOS_ASSERT(0 < logicalChildIndex);

		pexprEdge->AddRef();
		m_rgpedge[ul2 + innerJoinEdges] =
			GPOS_NEW(mp) SEdge(mp, pexprEdge, lojId);
		// this edge (ON predicate) is always associated with the right
		// child of the LOJ, whether it refers to it in the ON pred or not
		// Example: select * from t1 left outer join t2 on t1.a=5
		// We still want to associate this ON predicate with t2
		m_rgpedge[ul2 + innerJoinEdges]->m_pbs->ExchangeSet(logicalChildIndex);
	}

	// create the components (both inner and LOJs)
	for (ULONG ul = 0; ul < num_of_nary_children; ul++)
	{
		CExpression *expr = (*all_components)[ul];
		INT lojId = (NULL != childPredIndexes) ? *((*childPredIndexes)[ul]) : 0;

		AddComponent(mp, expr, lojId, EpSentinel /*position not used in DPv2*/,
					 ul);
	}

	ComputeEdgeCover();

	all_components->Release();
	innerJoinPredConjuncts->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::~CJoinOrder
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrder::~CJoinOrder()
{
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		m_rgpcomp[ul]->Release();
	}
	GPOS_DELETE_ARRAY(m_rgpcomp);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		m_rgpedge[ul]->Release();
	}
	GPOS_DELETE_ARRAY(m_rgpedge);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::ComputeEdgeCover
//
//	@doc:
//		Compute cover for each edge and also the index of edges associated
//		with each component
//
//---------------------------------------------------------------------------
void
CJoinOrder::ComputeEdgeCover()
{
	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		CExpression *pexpr = m_rgpedge[ulEdge]->m_pexpr;
		CColRefSet *pcrsUsed = pexpr->DeriveUsedColumns();

		for (ULONG ulComp = 0; ulComp < m_ulComps; ulComp++)
		{
			CExpression *pexprComp = m_rgpcomp[ulComp]->m_pexpr;
			CColRefSet *pcrsOutput = pexprComp->DeriveOutputColumns();

			if (!pcrsUsed->IsDisjoint(pcrsOutput))
			{
				(void) m_rgpcomp[ulComp]->m_edge_set->ExchangeSet(ulEdge);
				(void) m_rgpedge[ulEdge]->m_pbs->ExchangeSet(ulComp);
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::PcompCombine
//
//	@doc:
//		Combine the two given components using applicable edges
//
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent *
CJoinOrder::PcompCombine(SComponent *comp1, SComponent *comp2)
{
	GPOS_ASSERT(IsValidJoinCombination(comp1, comp2));
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp);
	CBitSet *edge_set = GPOS_NEW(m_mp) CBitSet(m_mp);

	pbs->Union(comp1->m_pbs);
	pbs->Union(comp2->m_pbs);

	// edges connecting with the current component
	edge_set->Union(comp1->m_edge_set);
	edge_set->Union(comp2->m_edge_set);

	// collect the list of conjuncts involved involved in the join from the list
	// of edges, making sure to separate those that are derived from LOJs
	CExpressionArray *loj_conjuncts = GPOS_NEW(m_mp) CExpressionArray(m_mp);
	CExpressionArray *other_conjuncts = GPOS_NEW(m_mp) CExpressionArray(m_mp);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (pedge->m_fUsed)
		{
			// edge is already used in result component
			continue;
		}

		if (pbs->ContainsAll(pedge->m_pbs))
		{
			// edge is subsumed by the cover of the combined component
			CExpression *pexpr = pedge->m_pexpr;
			pexpr->AddRef();
			if (0 < pedge->m_loj_num)
				loj_conjuncts->Append(pexpr);
			else
				other_conjuncts->Append(pexpr);
		}
	}

	CExpression *pexprChild1 = comp1->m_pexpr;
	CExpression *pexprChild2 = comp2->m_pexpr;

	CExpression *pexpr = NULL;
	INT parent_loj_id = NON_LOJ_DEFAULT_ID;
	EPosition position = EpSentinel;

	if (NULL == pexprChild1)
	{
		// first call to this function, we create a Select node
		parent_loj_id = comp2->ParentLojId();
		position = comp2->Position();

		GPOS_ASSERT(loj_conjuncts->Size() == 0);

		// combine all the conjuncts - since we are not creating a LOJ
		CUtils::AddRefAppend(loj_conjuncts, other_conjuncts);
		other_conjuncts->Release();

		CExpression *predicate =
			CPredicateUtils::PexprConjunction(m_mp, loj_conjuncts);
		pexpr = CUtils::PexprCollapseSelect(m_mp, pexprChild2, predicate);
		predicate->Release();
	}
	else
	{
		// not first call, we create an Inner Join or LOJ
		GPOS_ASSERT(NULL != pexprChild2);
		pexprChild2->AddRef();
		pexprChild1->AddRef();

		if (IsChildOfSameLOJ(comp1, comp2))
		{
			// if both the components are child of the same LOJ, ensure that the left child
			// stays left child and vice versa while creating the LOJ.
			// for this component we need not track the parent_loj_id, as we are only
			// concerned for joining LOJ childs with other non-LOJ components
			CExpression *pexprLeft = comp1->Position() == CJoinOrder::EpLeft
										 ? pexprChild1
										 : pexprChild2;
			CExpression *pexprRight = comp1->Position() == CJoinOrder::EpLeft
										  ? pexprChild2
										  : pexprChild1;

			// construct a LOJ only using predicates from an original LOJ; mixing LOJ
			// predicates with INNER join predicates could lead to wrong results
			// since they change the semantics of the outer join.
			CExpression *loj_predicate =
				CPredicateUtils::PexprConjunction(m_mp, loj_conjuncts);
			pexpr = CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(
				m_mp, pexprLeft, pexprRight, loj_predicate);

			// remaining predicates are place on top as a filter
			CExpression *filter_predicate =
				CPredicateUtils::PexprConjunction(m_mp, other_conjuncts);
			pexpr = CUtils::PexprLogicalSelect(m_mp, pexpr, filter_predicate);
		}
		else
		{
			if (comp1->ParentLojId() > NON_LOJ_DEFAULT_ID ||
				comp2->ParentLojId() > NON_LOJ_DEFAULT_ID)
			{
				// one of the component is the child of an LOJ, and can be joined with another relation
				// to create an Inner Join. for other non LOJ childs of NAry join, the parent loj id is
				// defaulted to 0, so assert the condition.
				GPOS_ASSERT_IMP(comp1->ParentLojId() > NON_LOJ_DEFAULT_ID,
								comp2->ParentLojId() == 0);
				GPOS_ASSERT_IMP(comp2->ParentLojId() > NON_LOJ_DEFAULT_ID,
								comp1->ParentLojId() == 0);

				parent_loj_id = NON_LOJ_DEFAULT_ID < comp1->ParentLojId()
									? comp1->ParentLojId()
									: comp2->ParentLojId();
				position = NON_LOJ_DEFAULT_ID < comp1->ParentLojId()
							   ? comp1->Position()
							   : comp2->Position();

				// since we only support joining the left child of LOJ to other relations in NAry Join,
				// we must not get right child of LOJ here, as that join combination must have been isolated
				// by IsValidJoinCombination earlier.
				GPOS_ASSERT(CJoinOrder::EpLeft == position);

				// we track if this join component contains the left child of LOJ,
				// so the parent loj id must be some positive non-zero number (i.e > 0)
				// for this join component
				GPOS_ASSERT(NON_LOJ_DEFAULT_ID < parent_loj_id);
			}

			GPOS_ASSERT(loj_conjuncts->Size() == 0);

			// combine all the conjuncts - since we are not creating a LOJ
			CUtils::AddRefAppend(loj_conjuncts, other_conjuncts);
			other_conjuncts->Release();
			CExpression *predicate =
				CPredicateUtils::PexprConjunction(m_mp, loj_conjuncts);
			pexpr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
				m_mp, pexprChild1, pexprChild2, predicate);
		}
	}
	// if the component has parent_loj_id > 0, it must be the left child or has the left child
	// of loj id indicated by parent_loj_id
	GPOS_ASSERT_IMP(NON_LOJ_DEFAULT_ID < parent_loj_id, EpLeft == position);
	SComponent *join_comp = GPOS_NEW(m_mp)
		SComponent(pexpr, pbs, edge_set, parent_loj_id, position);

	return join_comp;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::DeriveStats
//
//	@doc:
//		Helper function to derive stats on a given component
//
//---------------------------------------------------------------------------
void
CJoinOrder::DeriveStats(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (NULL == pexpr->Pstats())
	{
		CExpressionHandle exprhdl(m_mp);
		exprhdl.Attach(pexpr);
		exprhdl.DeriveStats(m_mp, m_mp, NULL /*prprel*/,
							NULL /*pdrgpstatCtxt*/);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::OsPrint
//
//	@doc:
//		Helper function to print a join order class
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::OsPrint(IOstream &os) const
{
	os << "Join Order: " << std::endl << "Edges: " << m_ulEdges << std::endl;

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		m_rgpedge[ul]->OsPrint(os);
		os << std::endl;
	}

	os << "Components: " << m_ulComps << std::endl;
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		os << (void *) m_rgpcomp[ul] << " - " << std::endl;
		m_rgpcomp[ul]->OsPrint(os);
	}

	return os;
}

BOOL
CJoinOrder::IsValidJoinCombination(SComponent *comp1, SComponent *comp2) const
{
	INT comp1_parent_loj_id = comp1->ParentLojId();
	INT comp2_parent_loj_id = comp2->ParentLojId();
	EPosition comp1_position = comp1->Position();
	EPosition comp2_position = comp2->Position();


	// Consider the below tree, for examples used:
	//+--CLogicalNAryJoin
	//	|--CLogicalGet "t1"
	//	|--CLogicalGet "t2"
	//	|--CLogicalLeftOuterJoin => LOJ 1
	//	|  |--CLogicalGet "t3" => {1, EpLeft}
	//	|  |--CLogicalGet "t4" => {1, EpRight}
	//	|  +--<Join Condition>
	//	|--CLogicalLeftOuterJoin => LOJ 2
	//	|  |--CLogicalGet "t5" => {2, EpLeft}
	//	|  |--CLogicalGet "t6" => {2, EpRight}
	//	|  +--<Join Condition>
	//	+--<Join Condition>

	if ((NON_LOJ_DEFAULT_ID == comp1_parent_loj_id &&
		 NON_LOJ_DEFAULT_ID == comp2_parent_loj_id))
	{
		// neither component contains any LOJs childs,
		// this is valid
		// example: CLogicalGet "t1" join CLogicalGet "t2"
		return true;
	}

	if (NON_LOJ_DEFAULT_ID < comp1_parent_loj_id &&
		NON_LOJ_DEFAULT_ID < comp2_parent_loj_id)
	{
		// both components contain LOJs child,
		// check whether they are referring to same LOJ
		if (comp1_parent_loj_id == comp2_parent_loj_id)
		{
			// one of them should be a left child and other one right child
			// example: CLogicalGet "t3" join CLogicalGet "t4" is valid
			GPOS_ASSERT(comp1_position != EpSentinel &&
						comp2_position != EpSentinel);
			if ((comp1_position == EpLeft && comp2_position == EpRight) ||
				(comp1_position == EpRight && comp2_position == EpLeft))
			{
				return true;
			}
		}
		// two components with children from different LOJs, this is not valid
		// example: CLogicalGet "t3" join CLogicalGet "t5"
		return false;
	}

	// one of the components contains one LOJ child without the sibling,
	// this is allowed if it is a left LOJ child
	// example 1: CLogicalGet "t1" join CLogicalGet "t3" is valid
	// example 2: CLogicalGet "t1" join CLogicalGet "t4 is not valid
	return comp1_position != EpRight && comp2_position != EpRight;
}

BOOL
CJoinOrder::IsChildOfSameLOJ(SComponent *comp1, SComponent *comp2) const
{
	// check if these components are inner and outer children of a same join
	BOOL child_of_same_loj = comp1->ParentLojId() == comp2->ParentLojId() &&
							 comp1->ParentLojId() != NON_LOJ_DEFAULT_ID &&
							 ((comp1->Position() == CJoinOrder::EpLeft &&
							   comp2->Position() == CJoinOrder::EpRight) ||
							  (comp1->Position() == CJoinOrder::EpRight &&
							   comp2->Position() == CJoinOrder::EpLeft));

	return child_of_same_loj;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::MarkUsedEdges
//
//	@doc:
//		Mark edges used by result component
//
//---------------------------------------------------------------------------
void
CJoinOrder::MarkUsedEdges(SComponent *pcomponent)
{
	GPOS_ASSERT(NULL != pcomponent);

	CExpression *pexpr = pcomponent->m_pexpr;

	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();
	if (0 == pexpr->Arity() || (COperator::EopLogicalSelect != eopid &&
								COperator::EopLogicalInnerJoin != eopid &&
								COperator::EopLogicalLeftOuterJoin != eopid))
	{
		// result component does not have a scalar child, e.g. a Get node
		return;
	}

	CBitSetIter edges_iter(*(pcomponent->m_edge_set));

	while (edges_iter.Advance())
	{
		SEdge *pedge = m_rgpedge[edges_iter.Bit()];
		if (pedge->m_fUsed)
		{
			continue;
		}

		if (pcomponent->m_pbs->ContainsAll(pedge->m_pbs))
		{
			pedge->m_fUsed = true;
		}
	}
}

void
CJoinOrder::AddComponent(CMemoryPool *mp, CExpression *expr, INT loj_id,
						 EPosition position, INT comp_num)
{
	expr->AddRef();
	SComponent *comp = GPOS_NEW(mp) SComponent(mp, expr, loj_id, position);
	m_rgpcomp[comp_num] = comp;
	// component always covers itself
	(void) m_rgpcomp[comp_num]->m_pbs->ExchangeSet(comp_num);
}
// EOF
