//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJoinOrder.h
//
//	@doc:
//		Join Order Generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrder_H
#define GPOPT_CJoinOrder_H

#include "gpos/base.h"
#include "gpos/io/IOstream.h"

#include "gpopt/operators/CExpression.h"

// id for component created for relational nodes which are not
// the child of LOJ
#define NON_LOJ_DEFAULT_ID 0

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJoinOrder
//
//	@doc:
//		Helper class for creating compact join orders
//
//---------------------------------------------------------------------------
class CJoinOrder
{
public:
	enum EPosition
	{
		EpLeft = 0,	 // left child of LOJ
		EpRight,	 // right child of LOJ

		EpSentinel
	};

	//---------------------------------------------------------------------------
	//	@struct:
	//		SEdge
	//
	//	@doc:
	//		Struct to capture edge
	//
	//---------------------------------------------------------------------------
	struct SEdge : public CRefCount
	{
		// cover of edge
		CBitSet *m_pbs;

		// associated conjunct
		CExpression *m_pexpr;

		// tracks if the associated join is a LOJ:
		// check the derived classes of CJoinOrder, but here is a high-level summary:
		// greedy, mincard: 0 means not an LOJ, 1 means this is an LOJ edge
		// DP: always 0
		// DPv2: 0 means inner join, >0: the index points to the entry in the array of ON predicates
		ULONG m_loj_num;

		// a flag to mark edge as used
		BOOL m_fUsed;

		// ctor
		SEdge(CMemoryPool *mp, CExpression *pexpr, ULONG loj_num);

		// dtor
		~SEdge();

		// print routine
		IOstream &OsPrint(IOstream &os) const;
	};


	//---------------------------------------------------------------------------
	//	@struct:
	//		SComponent
	//
	//	@doc:
	//		Struct to capture component
	//
	//---------------------------------------------------------------------------
	struct SComponent : public CRefCount
	{
		// cover
		CBitSet *m_pbs;

		// set of edges associated with this component (stored as indexes into m_rgpedge array)
		CBitSet *m_edge_set;

		// associated expression
		CExpression *m_pexpr;

		// a flag to component edge as used
		BOOL m_fUsed;

		// for greedy and mincard:
		//
		// number indicating that this component contains
		// child of LOJ x, where x is a an incremental id
		// assigned to LOJ operators found in the NAry Join.
		// it is populated only if you choose to explore alternatives
		// to find a join order by evaluating joins of LOJ outer child
		// with other relational (non LOJ) operators in NAry tree.
		// Consider the below NAry join tree, which indicates the value
		// of m_parent_loj_id and m_position in {x, y} for the
		// component created for LOJ childs.
		// respectively
		//+--CLogicalNAryJoin
		//	|--CLogicalGet "t1"
		//	|--CLogicalLeftOuterJoin => LOJ 1
		//	|  |--CLogicalGet "t2" => {1, EpLeft}
		//	|  |--CLogicalGet "t3" => {1, EpRight}
		//	|  +--<Join Condition>
		//	+--<Join Condition>
		// For the above tree, outer child of LOJ can be combined to "t1",
		// i.e the resulting tree can be
		//	+--CLogicalLeftOuterJoin
		//		|--CLogicalInnerJoin => {1, EpLeft}, See note below.
		//		|	|--CLogicalGet "t1"
		//		|	|--CLogicalGet "t2"
		//		|	+--<Join Condition>
		//		|--CLogicalGet "t3" => {1, EpRight}
		//		+--<Join Condition>
		// Note: Component created for the join between t1 and t2 will have
		// m_parent_loj_id = 1 and m_position = EpLeft, as it
		// contains the left child of LOJ 1.
		// This helps to track that inner join between t1 and t2
		// must be the left child of LOJ with t3.
		//
		// for DPv2:
		//
		// We have n children, and some of them (other than the first) may be right children of
		// non-inner joins. These non-inner join right children are numbered 1 to j, and that
		// number is stored here in m_parent_loj_id. Example:
		// T0 join T1 join T2 left join T3 join T4 left join T5
		// The components for T0 ... T5 will have the following m_parent_loj_id values:
		// T0  T1  T2  T3  T4  T5
		//  0   0   0   1   0   2
		INT m_parent_loj_id;

		// enum indicating that this component contains left or
		// right child of the LOJ
		// (used for greedy and mincard, not used for DPv2)
		EPosition m_position;

		// ctor
		SComponent(CMemoryPool *mp, CExpression *expr,
				   INT parent_loj_id = NON_LOJ_DEFAULT_ID,
				   EPosition position = EpSentinel);

		// ctor
		SComponent(CExpression *expr, CBitSet *pbs, CBitSet *edge_set,
				   INT parent_loj_id = NON_LOJ_DEFAULT_ID,
				   EPosition position = EpSentinel);

		// dtor
		~SComponent();

		// get parent loj id
		INT
		ParentLojId()
		{
			return m_parent_loj_id;
		}

		// what is or must be the position of this component with
		// respect to parent LOJ
		EPosition
		Position()
		{
			return m_position;
		}

		// print routine
		IOstream &OsPrint(IOstream &os) const;
	};

protected:
	// memory pool
	CMemoryPool *m_mp;

	// edges
	SEdge **m_rgpedge;

	// number of edges
	ULONG m_ulEdges;

	// components
	SComponent **m_rgpcomp;

	// number of components
	ULONG m_ulComps;

	// should we include loj childs for evaluating
	// join order
	BOOL m_include_loj_childs;

	// compute cover of each edge
	void ComputeEdgeCover();

	// combine the two given components using applicable edges
	SComponent *PcompCombine(SComponent *comp1, SComponent *comp2);

	// derive stats on a given component
	virtual void DeriveStats(CExpression *pexpr);

	// mark edges used by expression
	void MarkUsedEdges(SComponent *comp);

	// add component to to component array
	void AddComponent(CMemoryPool *mp, CExpression *expr, INT loj_id,
					  EPosition position, INT comp_num);

private:
	// private copy ctor
	CJoinOrder(const CJoinOrder &);

public:
	// ctor used in MinCard, Greedy and DP xforms
	CJoinOrder(CMemoryPool *mp, CExpressionArray *pdrgpexprComponents,
			   CExpressionArray *pdrgpexprConjuncts,
			   BOOL include_outer_join_childs);

	// ctor used in CXformExpandNAryJoinDPv2
	CJoinOrder(CMemoryPool *mp, CExpressionArray *pdrgpexprComponents,
			   CExpressionArray *innerJoinPredConjuncts,
			   CExpressionArray *onPreds, ULongPtrArray *childPredIndexes);

	// dtor
	virtual ~CJoinOrder();

	// print function
	virtual IOstream &OsPrint(IOstream &) const;

	// is this a valid join combination
	BOOL IsValidJoinCombination(SComponent *comp1, SComponent *comp2) const;

	// are these childs of the same LOJ
	BOOL IsChildOfSameLOJ(SComponent *comp1, SComponent *comp2) const;

};	// class CJoinOrder

}  // namespace gpopt

#endif	// !GPOPT_CJoinOrder_H

// EOF
