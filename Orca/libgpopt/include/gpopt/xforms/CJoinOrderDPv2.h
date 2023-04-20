//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2019 Pivotal Inc.
//
//	@filename:
//		CJoinOrderDPv2.h
//
//	@doc:
//		Dynamic programming-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderDPv2_H
#define GPOPT_CJoinOrderDPv2_H

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CHashMap.h"
#include "gpos/io/IOstream.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/xforms/CJoinOrder.h"


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJoinOrderDPv2
//
//	@doc:
//		Helper class for creating join orders using dynamic programming
//
//		Some terminology:
//
//		NIJ:	Non-inner join. This is sometimes used instead of left join,
//				since we anticipate to extend this code to semi-joins and other
//				types of joins.
//		Atom:	A child of the NAry join. This could be a table or some
//				other operator like a groupby or a full outer join.
//		Group:	A set of atoms (called a "component" in DPv1). Once we generate
//				a result expression, each of these sets will be associated
//				with a CGroup in MEMO.
//---------------------------------------------------------------------------
class CJoinOrderDPv2 : public CJoinOrder
{
private:
	// a heap keeping the k lowest-cost objects in an array of class A
	// A is a CDynamicPtrArray
	// E is the entry type of the array and it has a method CDouble DCost()
	// See https://en.wikipedia.org/wiki/Binary_heap for details
	// (with the added feature of returning only the top k).
	template <class A, class E>
	class KHeap : public CRefCount
	{
	private:
		A *m_topk;
		CMemoryPool *m_mp;
		ULONG m_k;
		BOOL m_is_heapified;
		ULONG m_num_returned;

		// the parent index is (ix-1)/2, except for 0
		ULONG
		parent(ULONG ix)
		{
			return (0 < ix ? (ix - 1) / 2 : m_topk->Size());
		}

		// children are at indexes 2*ix + 1 and 2*ix + 2
		ULONG
		left_child(ULONG ix)
		{
			return 2 * ix + 1;
		}
		ULONG
		right_child(ULONG ix)
		{
			return 2 * ix + 2;
		}

		// does the parent/child exist?
		BOOL
		exists(ULONG ix)
		{
			return ix < m_topk->Size();
		}
		// cost of an entry (this class implements a Min-Heap)
		CDouble
		cost(ULONG ix)
		{
			return (*m_topk)[ix]->GetCostForHeap();
		}

		// push node ix in the tree down into its child tree as much as needed
		void
		HeapifyDown(ULONG ix)
		{
			ULONG left_child_ix = left_child(ix);
			ULONG right_child_ix = right_child(ix);
			ULONG min_element_ix = ix;

			if (exists(left_child_ix) && cost(left_child_ix) < cost(ix))
				// left child is better than parent, it becomes the new candidate
				min_element_ix = left_child_ix;

			if (exists(right_child_ix) &&
				cost(right_child_ix) < cost(min_element_ix))
				// right child is better than min(parent, left child)
				min_element_ix = right_child_ix;

			if (min_element_ix != ix)
			{
				// make the lowest of { parent, left child, right child } the new root
				m_topk->Swap(ix, min_element_ix);
				HeapifyDown(min_element_ix);
			}
		}

		// pull node ix in the tree up as much as needed
		void
		HeapifyUp(ULONG ix)
		{
			ULONG parent_ix = parent(ix);

			if (!exists(parent_ix))
				return;

			if (cost(ix) < cost(parent_ix))
			{
				m_topk->Swap(ix, parent_ix);
				HeapifyUp(parent_ix);
			}
		}

		// Convert the array into a heap, heapify-down all interior nodes of the tree, bottom-up.
		// Note that we keep all the entries, not just the top k, since our k-heaps are short-lived.
		// You can only retrieve the top k with RemoveBestElement(), though.
		void
		Heapify()
		{
			// the parent of the last node is the last node in the tree that is a parent
			ULONG start_ix = parent(m_topk->Size() - 1);

			// now work our way up to the root, calling HeapifyDown
			for (ULONG ix = start_ix; exists(ix); ix--)
				HeapifyDown(ix);

			m_is_heapified = true;
		}

	public:
		KHeap(CMemoryPool *mp, ULONG k)
			: m_mp(mp), m_k(k), m_is_heapified(false), m_num_returned(0)
		{
			m_topk = GPOS_NEW(m_mp) A(m_mp);
		}

		~KHeap()
		{
			m_topk->Release();
		}

		void
		Insert(E *elem)
		{
			GPOS_ASSERT(NULL != elem);
			// since the cost may change as we find more expressions in the group,
			// we just append to the array now and heapify at the end
			GPOS_ASSERT(!m_is_heapified);
			m_topk->Append(elem);

			// this is dead code at the moment, but other users might want to
			// heapify and then insert additional items
			if (m_is_heapified)
			{
				HeapifyUp(m_topk->Size() - 1);
			}
		}

		// remove the next of the top k elements, sorted ascending by cost
		E *
		RemoveBestElement()
		{
			if (0 == m_topk->Size() || m_k <= m_num_returned)
			{
				return NULL;
			}

			m_num_returned++;

			return RemoveNextElement();
		}

		// remove the next best element, without the top k limit
		E *
		RemoveNextElement()
		{
			if (0 == m_topk->Size())
			{
				return NULL;
			}

			if (!m_is_heapified)
				Heapify();

			// we want to remove and return the root of the tree, which is the best element

			// first, swap the root with the last element in the array
			m_topk->Swap(0, m_topk->Size() - 1);

			// now remove the new last element, which is the real root
			E *result = m_topk->RemoveLast();

			// then push the new first element down to the correct place
			HeapifyDown(0);

			return result;
		}

		ULONG
		Size()
		{
			return m_topk->Size();
		}

		BOOL
		IsLimitExceeded()
		{
			return m_topk->Size() + m_num_returned > m_k;
		}

		void
		Clear()
		{
			m_topk->Clear();
			m_is_heapified = false;
			m_num_returned = 0;
		}
	};

	// Data structures for DPv2 join enumeration:
	//
	// Each level l is the set of l-way joins we are considering.
	// Level 1 describes the "atoms" the leaves of the original NAry join we are transforming.
	//
	// Each level consists of a set (an array) of "groups". A group represents what may eventually
	// become a group in the MEMO structure, if it is a part of one of the generated top k
	// expressions at the top level. It is a set of atoms to be joined (in any order).
	// The SGroupInfo struct contains the bitset representing the atoms, the cardinality from the
	// derived statistics and an array of SExpressionInfo structs.
	//
	// Each SExpressionInfo struct describes an expression in a group. Besides a CExpression,
	// it also has the SGroupInfo and SExpressionInfo of the children. Each SExpressionInfo
	// also has a property. The SExpressionInfo entries in a group all have different properties.
	// We only keep expressions that are not dominated by another expression, meaning that there
	// is no other expression that can produce a superset of the properties for a less or equal
	// cost.
	//
	//
	//           SLevelInfo
	//           +---------------------+
	// Level n:  | SGroupInfo array ---+----> SGroupInfo
	//           | optional top k      |      +------------------+
	//           +---------------------+      | Atoms (bitset)   |
	//                                        | cardinality      |
	//                                        | SExpressionInfo  |
	//                                        |      array       |
	//                                        +--------+---------+
	//                                                 v
	//                                          SExpressionInfo
	//                                          +------------------------+
	//                                          | CExpression            |
	//                                          | child SExpressionInfos |
	//                                          | properties             |
	//                                          +------------------------+
	//                                          +------------------------+
	//                                          | CExpression            |
	//                                          | child SExpressionInfos |
	//                                          | properties             |
	//                                          +------------------------+
	//                                           ...
	//                                          +------------------------+
	//                                          | CExpression            |
	//                                          | child SExpressionInfos |
	//                                          | properties             |
	//                                          +------------------------+
	//           ...
	//
	//           SLevelInfo
	//           +---------------------+
	// Level 1:  | SGroupInfo array ---+----> SGroupInfo                        SGroupInfo
	//           | optional top k      |      +------------------+              +------------------+
	//           +---------------------+      | Atoms (bitset)   |              | Atoms (bitset)   |
	//                                        | cardinality      +--------------+ cardinality      |
	//                                        | ExpressionInfo   |              | ExpressionInfo   |
	//                                        |      array       |              |      array       |
	//                                        +--------+---------+              +--------+---------+
	//                                                 v                                 v
	//                                          SExpressionInfo                   SExpressionInfo
	//                                          +------------------------+        +------------------------+
	//                                          | CExpression            |        | CExpression            |
	//                                          | child SExpressionInfos |        | child SExpressionInfos |
	//                                          | properties             |        | properties             |
	//                                          +------------------------+        +------------------------+
	//

	// forward declarations, circular reference
	struct SGroupInfo;
	struct SExpressionInfo;

	// Join enumeration algorithm properties, these can be added if an expression satisfies more than one
	// consider these as constants, not as a true enum
	// note that the numbers (other than the first) must be powers of 2,
	// since we add them to make composite properties!!!
	// Note also that query, mincard and GreedyAvoidXProd are all greedy algorithms.
	// Sorry for the confusion with the term "greedy" used in the optimizer_join_order guc
	// and the CXformExpandNAryJoinGreedy classes, where they refer to one type of greedy
	// algorithm that avoids cross products.
	enum JoinOrderPropType
	{
		EJoinOrderAny = 0,	// the overall best solution (used for exhaustive2)
		EJoinOrderQuery = 1,	// this expression uses the "query" join order
		EJoinOrderMincard = 2,	// this expression has the "mincard" property
		EJoinOrderGreedyAvoidXProd =
			4,	// best "greedy" expression with minimal cross products
		EJoinOrderHasPS =
			8,	// best expression with special consideration for DPE
		EJoinOrderDP = 16,	// best solution using DP
		EJoinOrderStats =
			32	// this expression is used to calculate the statistics
				// (row count) for the group
	};

	// properties of an expression in the DP structure (also used as required properties)
	struct SExpressionProperties
	{
		// the join order enumeration algorithm for which this is a solution
		// (exhaustive enumeration, can use any of these: EJoinOrderAny)
		ULONG m_join_order;

		SExpressionProperties(ULONG join_order_properties)
			: m_join_order(join_order_properties)
		{
		}

		BOOL
		Satisfies(ULONG pt)
		{
			return pt == (m_join_order & pt);
		}
		void
		Add(const SExpressionProperties &p)
		{
			m_join_order |= p.m_join_order;
		}
		BOOL
		IsGreedy()
		{
			return 0 != (m_join_order & (EJoinOrderQuery + EJoinOrderMincard +
										 EJoinOrderGreedyAvoidXProd));
		}
	};

	// a simple wrapper of an SGroupInfo * plus an index into its array of SExpressionInfos
	// this identifies a group and one expression belonging to that group
	struct SGroupAndExpression
	{
		SGroupInfo *m_group_info;
		ULONG m_expr_index;

		SGroupAndExpression()
			: m_group_info(NULL), m_expr_index(gpos::ulong_max)
		{
		}
		SGroupAndExpression(SGroupInfo *g, ULONG ix)
			: m_group_info(g), m_expr_index(ix)
		{
		}
		SGroupAndExpression(const SGroupAndExpression &other)
			: m_group_info(other.m_group_info), m_expr_index(other.m_expr_index)
		{
		}
		SExpressionInfo *
		GetExprInfo() const
		{
			return (*m_group_info->m_best_expr_info_array)[m_expr_index];
		}
		BOOL
		IsValid()
		{
			return NULL != m_group_info && gpos::ulong_max != m_expr_index;
		}
		BOOL
		operator==(const SGroupAndExpression &other) const
		{
			return m_group_info == other.m_group_info &&
				   m_expr_index == other.m_expr_index;
		}
	};

	// description of an expression in the DP environment
	// left and right child of join expressions point to
	// child groups + expressions
	struct SExpressionInfo : public CRefCount
	{
		// the expression
		CExpression *m_expr;

		// left/right child group/expr info (group for left/right child of m_expr),
		// we do not keep a refcount for these
		SGroupAndExpression m_left_child_expr;
		SGroupAndExpression m_right_child_expr;
		// derived properties of this expression
		SExpressionProperties m_properties;

		// in the future, we may add more properties relevant to the cost here,
		// like distribution spec, partition selectors

		// Stores part keys for atoms that are partitioned tables. NULL otherwise.
		CPartKeysArray *m_atom_part_keys_array;

		// cost of the expression
		CDouble m_cost;

		//cost adjustment for the effect of partition selectors, this is always <= 0.0
		CDouble m_cost_adj_PS;

		// base table rows, -1 if not atom or get/select
		CDouble m_atom_base_table_rows;

		// stores atom ids that are fufilled by a PS in this expression
		CBitSet *m_contain_PS;

		SExpressionInfo(CMemoryPool *mp, CExpression *expr,
						const SGroupAndExpression &left_child_expr_info,
						const SGroupAndExpression &right_child_expr_info,
						SExpressionProperties &properties)
			: m_expr(expr),
			  m_left_child_expr(left_child_expr_info),
			  m_right_child_expr(right_child_expr_info),
			  m_properties(properties),
			  m_atom_part_keys_array(NULL),
			  m_cost(0.0),
			  m_cost_adj_PS(0.0),
			  m_atom_base_table_rows(-1.0),
			  m_contain_PS(NULL)

		{
			m_contain_PS = GPOS_NEW(mp) CBitSet(mp);
			this->UnionPSProperties(left_child_expr_info.GetExprInfo());
			this->UnionPSProperties(right_child_expr_info.GetExprInfo());
		}

		SExpressionInfo(CMemoryPool *mp, CExpression *expr,
						SExpressionProperties &properties)
			: m_expr(expr),
			  m_properties(properties),
			  m_atom_part_keys_array(NULL),
			  m_cost(0.0),
			  m_cost_adj_PS(0.0),
			  m_atom_base_table_rows(-1.0),
			  m_contain_PS(NULL)
		{
			m_contain_PS = GPOS_NEW(mp) CBitSet(mp);
		}

		~SExpressionInfo()
		{
			m_expr->Release();
			CRefCount::SafeRelease(m_contain_PS);
		}

		// cost (use -1 for greedy solutions to ensure we keep all of them)
		CDouble
		GetCostForHeap()
		{
			return m_properties.IsGreedy() ? -1.0 : GetCost();
		}

		CDouble
		GetCost()
		{
			return m_cost + m_cost_adj_PS;
		}

		void
		UnionPSProperties(SExpressionInfo *other)
		{
			m_contain_PS->Union(other->m_contain_PS);
		}
		BOOL
		ChildrenAreEqual(const SExpressionInfo &other) const
		{
			return m_left_child_expr == other.m_left_child_expr &&
				   m_right_child_expr == other.m_right_child_expr;
		}
	};

	typedef CDynamicPtrArray<SExpressionInfo, CleanupRelease<SExpressionInfo> >
		SExpressionInfoArray;

	//---------------------------------------------------------------------------
	//	@struct:
	//		SGroupInfo
	//
	//	@doc:
	//		Struct containing a bitset, representing a group, its best expression, and cost
	//
	//---------------------------------------------------------------------------
	struct SGroupInfo : public CRefCount
	{
		// the set of atoms, this uniquely identifies the group
		CBitSet *m_atoms;
		// infos of the best (lowest cost) expressions (so far, if at the current level)
		// for each interesting property
		SExpressionInfoArray *m_best_expr_info_array;
		CDouble m_cardinality;
		CDouble m_lowest_expr_cost;

		SGroupInfo(CMemoryPool *mp, CBitSet *atoms)
			: m_atoms(atoms), m_cardinality(-1.0), m_lowest_expr_cost(-1.0)
		{
			m_best_expr_info_array = GPOS_NEW(mp) SExpressionInfoArray(mp);
		}

		~SGroupInfo()
		{
			m_atoms->Release();
			m_best_expr_info_array->Release();
		}

		BOOL
		IsAnAtom()
		{
			return 1 == m_atoms->Size();
		}
		CDouble
		GetCostForHeap()
		{
			return m_lowest_expr_cost;
		}
	};

	// dynamic array of SGroupInfo, where each index represents an alternative group of a given level k
	typedef CDynamicPtrArray<SGroupInfo, CleanupRelease<SGroupInfo> >
		SGroupInfoArray;

	// info for a join level, the set of all groups representing <m_level>-way joins
	struct SLevelInfo : public CRefCount
	{
		ULONG m_level;
		SGroupInfoArray *m_groups;
		KHeap<SGroupInfoArray, SGroupInfo> *m_top_k_groups;

		SLevelInfo(ULONG level, SGroupInfoArray *groups)
			: m_level(level), m_groups(groups), m_top_k_groups(NULL)
		{
		}

		~SLevelInfo()
		{
			m_groups->Release();
			CRefCount::SafeRelease(m_top_k_groups);
		}
	};

	// hashing function
	static ULONG
	UlHashBitSet(const CBitSet *pbs)
	{
		GPOS_ASSERT(NULL != pbs);

		return pbs->HashValue();
	}

	// equality function
	static BOOL
	FEqualBitSet(const CBitSet *pbsFst, const CBitSet *pbsSnd)
	{
		GPOS_ASSERT(NULL != pbsFst);
		GPOS_ASSERT(NULL != pbsSnd);

		return pbsFst->Equals(pbsSnd);
	}

	typedef CHashMap<CExpression, SEdge, CExpression::HashValue, CUtils::Equals,
					 CleanupRelease<CExpression>, CleanupRelease<SEdge> >
		ExpressionToEdgeMap;

	// dynamic array of SGroupInfos
	typedef CHashMap<CBitSet, SGroupInfo, UlHashBitSet, FEqualBitSet,
					 CleanupRelease<CBitSet>, CleanupRelease<SGroupInfo> >
		BitSetToGroupInfoMap;

	// iterator over group infos in a level
	typedef CHashMapIter<CBitSet, SGroupInfo, UlHashBitSet, FEqualBitSet,
						 CleanupRelease<CBitSet>, CleanupRelease<SGroupInfo> >
		BitSetToGroupInfoMapIter;

	// dynamic array of SLevelInfos, where each index represents the level
	typedef CDynamicPtrArray<SLevelInfo, CleanupRelease<SLevelInfo> >
		DPv2Levels;

	// an array of an array of groups, organized by level at the first array dimension,
	// main data structure for dynamic programming
	DPv2Levels *m_join_levels;

	// map to find the associated edge in the join graph from a join predicate
	ExpressionToEdgeMap *m_expression_to_edge_map;

	// map to check whether a DPv2 group already exists
	BitSetToGroupInfoMap *m_bitset_to_group_info_map;

	// ON predicates for NIJs (non-inner joins, e.g. LOJs)
	// currently NIJs are LOJs only, this may change in the future
	// if/when we add semijoins, anti-semijoins and relatives
	CExpressionArray *m_on_pred_conjuncts;

	// association between logical children and inner join/ON preds
	// (which of the logical children are right children of NIJs and what ON predicates are they using)
	ULongPtrArray *m_child_pred_indexes;

	// for each non-inner join (entry in m_on_pred_conjuncts), the required atoms on the left
	CBitSetArray *m_non_inner_join_dependencies;

	// top K expressions at the top level
	KHeap<SExpressionInfoArray, SExpressionInfo> *m_top_k_expressions;

	// top K expressions at top level that contain promising dynamic partiion selectors
	// if there are no promising dynamic partition selectors, this will be empty
	KHeap<SExpressionInfoArray, SExpressionInfo> *m_top_k_part_expressions;

	// current penalty for cross products (depends on enumeration algorithm)
	CDouble m_cross_prod_penalty;

	// outer references, if any
	CColRefSet *m_outer_refs;

	CMemoryPool *m_mp;

	SLevelInfo *
	Level(ULONG l)
	{
		return (*m_join_levels)[l];
	}

	// build expression linking given groups
	CExpression *PexprBuildInnerJoinPred(CBitSet *pbsFst, CBitSet *pbsSnd);

	// compute cost of a join expression in a group
	void ComputeCost(SExpressionInfo *expr_info, CDouble join_cardinality);

	// if we need to keep track of used edges, make a map that
	// speeds up this usage check
	void PopulateExpressionToEdgeMapIfNeeded();

	// add a select node with any remaining edges (predicates) that have
	// not been incorporated in the join tree
	CExpression *AddSelectNodeForRemainingEdges(CExpression *join_expr);

	// mark all the edges used in a join tree
	void RecursivelyMarkEdgesAsUsed(CExpression *expr);

	// enumerate all possible joins between left_level-way joins on the left side
	// and right_level-way joins on the right side, resulting in left_level + right_level-way joins
	void SearchJoinOrders(ULONG left_level, ULONG right_level);

	void GreedySearchJoinOrders(ULONG left_level, JoinOrderPropType algo);

	virtual void DeriveStats(CExpression *pexpr);

	// create a CLogicalJoin and a CExpression to join two groups, for a required property
	SExpressionInfo *GetJoinExprForProperties(
		SGroupInfo *left_child, SGroupInfo *right_child,
		SExpressionProperties &required_properties);

	// get a join expression from two child groups with specified child expressions
	SExpressionInfo *GetJoinExpr(const SGroupAndExpression &left_child_expr,
								 const SGroupAndExpression &right_child_expr,
								 SExpressionProperties &result_properties);

	// does "prop" provide all the properties of "other_prop" plus maybe more?
	BOOL IsASupersetOfProperties(SExpressionProperties &prop,
								 SExpressionProperties &other_prop);

	// is one of the properties a subset of the other or are they disjoint?
	BOOL ArePropertiesDisjoint(SExpressionProperties &prop,
							   SExpressionProperties &other_prop);

	// get best expression in a group for a given set of properties
	SGroupAndExpression GetBestExprForProperties(SGroupInfo *group_info,
												 SExpressionProperties &props);

	// add a new property to an existing predicate
	void AddNewPropertyToExpr(SExpressionInfo *expr_info,
							  SExpressionProperties props);

	// enumerate bushy joins (joins where both children are also joins) of level "current_level"
	void SearchBushyJoinOrders(ULONG current_level);

	// look up an existing group or create a new one, with an expression to be used for stats
	SGroupInfo *LookupOrCreateGroupInfo(SLevelInfo *levelInfo, CBitSet *atoms,
										SExpressionInfo *stats_expr_info);
	// add a new expression to a group, unless there already is an existing expression that dominates it
	void AddExprToGroupIfNecessary(SGroupInfo *group_info,
								   SExpressionInfo *new_expr_info);

	void PopulateDPEInfo(SExpressionInfo *join_expr_info,
						 SGroupInfo *left_group_info,
						 SGroupInfo *right_group_info);

	void FinalizeDPLevel(ULONG level);

	SGroupInfoArray *
	GetGroupsForLevel(ULONG level) const
	{
		return (*m_join_levels)[level]->m_groups;
	}

	ULONG FindLogicalChildByNijId(ULONG nij_num);
	static ULONG NChooseK(ULONG n, ULONG k);
	BOOL LevelIsFull(ULONG level);

	void EnumerateDP();
	void EnumerateQuery();
	void FindLowestCardTwoWayJoin(JoinOrderPropType prop_type);
	void EnumerateMinCard();
	void EnumerateGreedyAvoidXProd();

public:
	// ctor
	CJoinOrderDPv2(CMemoryPool *mp, CExpressionArray *pdrgpexprAtoms,
				   CExpressionArray *innerJoinConjuncts,
				   CExpressionArray *onPredConjuncts,
				   ULongPtrArray *childPredIndexes, CColRefSet *outerRefs);

	// dtor
	virtual ~CJoinOrderDPv2();

	// main handler
	virtual void PexprExpand();

	CExpression *GetNextOfTopK();

	// check for NIJs
	BOOL IsRightChildOfNIJ(SGroupInfo *groupInfo,
						   CExpression **onPredToUse = NULL,
						   CBitSet **requiredBitsOnLeft = NULL);

	// print function
	virtual IOstream &OsPrint(IOstream &) const;

	IOstream &OsPrintProperty(IOstream &, SExpressionProperties &) const;

#ifdef GPOS_DEBUG
	void DbgPrint();
#endif

};	// class CJoinOrderDPv2

}  // namespace gpopt

#endif	// !GPOPT_CJoinOrderDPv2_H

// EOF
