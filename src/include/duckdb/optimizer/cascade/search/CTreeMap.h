//---------------------------------------------------------------------------
//	@filename:
//		CTreeMap.h
//
//	@doc:
//		Map of tree components to count, rank, and unrank abstract trees;
//
//		For description of algorithm, see also:
//
//			F. Waas, C. Galindo-Legaria, "Counting, Enumerating, and
//			Sampling of Execution Plans in a Cost-Based Query Optimizer",
//			ACM SIGMOD, 2000
//---------------------------------------------------------------------------
#ifndef GPOPT_CTreeMap_H
#define GPOPT_CTreeMap_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CHashMap.h"
#include "duckdb/optimizer/cascade/common/CHashMapIter.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CTreeMap
//
//	@doc:
//		Lookup table for counting/unranking of trees;
//
//		Enables client to associate objects of type T (e.g. CGroupExpression)
//		with a topology solely given by edges between the object. The
//		unranking utilizes client provided function and generates results of
//		type R (e.g., CExpression);
//		U is a global context accessible to recursive rehydrate calls.
//		Pointers to objects of type U are passed through PrUnrank calls to the
//		rehydrate function of type PrFn.
//
//---------------------------------------------------------------------------
template <class T, class R, class U, ULONG (*HashFn)(const T *),
		  BOOL (*EqFn)(const T *, const T *)>
class CTreeMap
{
	// array of source pointers (sources owned by 3rd party)
	typedef CDynamicPtrArray<T, CleanupNULL> DrgPt;

	// array of result pointers (results owned by the tree we unrank)
	typedef CDynamicPtrArray<R, CleanupRelease<R> > DrgPr;

	// generic rehydrate function
	typedef R *(*PrFn)(CMemoryPool *, T *, DrgPr *, U *);

private:
	// fwd declaration
	class CTreeNode;

	// arrays of internal nodes
	typedef CDynamicPtrArray<CTreeNode, CleanupNULL> CTreeNodeArray;
	typedef CDynamicPtrArray<CTreeNodeArray, CleanupRelease> CTreeNode2dArray;

	//---------------------------------------------------------------------------
	//	@class:
	//		STreeLink
	//
	//	@doc:
	//		Internal structure to monitor tree links for duplicate detection
	//		purposes
	//
	//---------------------------------------------------------------------------
	struct STreeLink
	{
	private:
		// parent node
		const T *m_ptParent;

		// child index
		ULONG m_ulChildIndex;

		// child node
		const T *m_ptChild;

	public:
		// ctor
		STreeLink(const T *ptParent, ULONG child_index, const T *ptChild)
			: m_ptParent(ptParent),
			  m_ulChildIndex(child_index),
			  m_ptChild(ptChild)
		{
			GPOS_ASSERT(NULL != ptParent);
			GPOS_ASSERT(NULL != ptChild);
		}

		// dtor
		virtual ~STreeLink()
		{
		}

		// hash function
		static ULONG
		HashValue(const STreeLink *ptlink)
		{
			ULONG ulHashParent = HashFn(ptlink->m_ptParent);
			ULONG ulHashChild = HashFn(ptlink->m_ptChild);
			ULONG ulHashChildIndex =
				gpos::HashValue<ULONG>(&ptlink->m_ulChildIndex);

			return CombineHashes(ulHashParent,
								 CombineHashes(ulHashChild, ulHashChildIndex));
		}

		// equality function
		static BOOL
		Equals(const STreeLink *ptlink1, const STreeLink *ptlink2)
		{
			return EqFn(ptlink1->m_ptParent, ptlink2->m_ptParent) &&
				   EqFn(ptlink1->m_ptChild, ptlink2->m_ptChild) &&
				   ptlink1->m_ulChildIndex == ptlink2->m_ulChildIndex;
		}
	};	// struct STreeLink

	//---------------------------------------------------------------------------
	//	@class:
	//		CTreeNode
	//
	//	@doc:
	//		Internal structure to manage source objects and their topology
	//
	//---------------------------------------------------------------------------
	class CTreeNode
	{
	private:
		// state of tree node during counting alternatives
		enum ENodeState
		{
			EnsUncounted,  // counting not initiated
			EnsCounting,   // counting in progress
			EnsCounted,	   // counting complete

			EnsSentinel
		};

		// memory pool
		CMemoryPool *m_mp;

		// id of node
		ULONG m_ul;

		// element
		const T *m_value;

		// array of children arrays
		CTreeNode2dArray *m_pdrgdrgptn;

		// number of trees rooted in this node
		ULLONG m_ullCount;

		// number of incoming edges
		ULONG m_ulIncoming;

		// node state used for counting alternatives
		ENodeState m_ens;

		// total tree count for a given child
		ULLONG
		UllCount(ULONG ulChild)
		{
			GPOS_CHECK_STACK_SIZE;

			ULLONG ull = 0;

			ULONG ulCandidates = (*m_pdrgdrgptn)[ulChild]->Size();
			for (ULONG ulAlt = 0; ulAlt < ulCandidates; ulAlt++)
			{
				CTreeNode *ptn = (*(*m_pdrgdrgptn)[ulChild])[ulAlt];
				ULLONG ullCount = ptn->UllCount();
				ull = gpos::Add(ull, ullCount);
			}

			return ull;
		}

		// rehydrate tree
		R *
		PrUnrank(CMemoryPool *mp, PrFn prfn, U *pU, ULONG ulChild,
				 ULLONG ullRank)
		{
			GPOS_CHECK_STACK_SIZE;
			GPOS_ASSERT(ullRank < UllCount(ulChild));

			CTreeNodeArray *pdrgptn = (*m_pdrgdrgptn)[ulChild];
			ULONG ulCandidates = pdrgptn->Size();

			CTreeNode *ptn = NULL;

			for (ULONG ul = 0; ul < ulCandidates; ul++)
			{
				ptn = (*pdrgptn)[ul];
				ULLONG ullLocalCount = ptn->UllCount();

				if (ullRank < ullLocalCount)
				{
					// ullRank is now local rank for the child
					break;
				}

				ullRank -= ullLocalCount;
			}

			GPOS_ASSERT(NULL != ptn);
			return ptn->PrUnrank(mp, prfn, pU, ullRank);
		}

	public:
		// ctor
		CTreeNode(CMemoryPool *mp, ULONG ul, const T *value)
			: m_mp(mp),
			  m_ul(ul),
			  m_value(value),
			  m_pdrgdrgptn(NULL),
			  m_ullCount(gpos::ullong_max),
			  m_ulIncoming(0),
			  m_ens(EnsUncounted)
		{
			m_pdrgdrgptn = GPOS_NEW(mp) CTreeNode2dArray(mp);
		}

		// dtor
		~CTreeNode()
		{
			m_pdrgdrgptn->Release();
		}

		// add child alternative
		void
		Add(ULONG ulPos, CTreeNode *ptn)
		{
			GPOS_ASSERT(!FCounted() &&
						"Adding edges after counting not meaningful");

			// insert any child arrays skipped so far; make sure we have a dense
			// array up to the position of ulPos
			ULONG length = m_pdrgdrgptn->Size();
			for (ULONG ul = length; ul <= ulPos; ul++)
			{
				CTreeNodeArray *pdrg = GPOS_NEW(m_mp) CTreeNodeArray(m_mp);
				m_pdrgdrgptn->Append(pdrg);
			}

			// increment count of incoming edges
			ptn->m_ulIncoming++;

			// insert to appropriate array
			CTreeNodeArray *pdrg = (*m_pdrgdrgptn)[ulPos];
			GPOS_ASSERT(NULL != pdrg);
			pdrg->Append(ptn);
		}

		// accessor
		const T *
		Value() const
		{
			return m_value;
		}

		// number of trees rooted in this node
		ULLONG
		UllCount()
		{
			GPOS_CHECK_STACK_SIZE;

			GPOS_ASSERT(EnsCounting != m_ens && "cycle in graph detected");

			if (!FCounted())
			{
				// initiate counting on current node
				m_ens = EnsCounting;

				ULLONG ullCount = 1;

				ULONG arity = m_pdrgdrgptn->Size();
				for (ULONG ulChild = 0; ulChild < arity; ulChild++)
				{
					ULLONG ull = UllCount(ulChild);
					if (0 == ull)
					{
						// if current child has no alternatives, the parent cannot have alternatives
						ullCount = 0;
						break;
					}

					// otherwise, multiply number of child alternatives by current count
					ullCount = gpos::Multiply(ullCount, ull);
				}

				// counting is complete
				m_ullCount = ullCount;
				m_ens = EnsCounted;
			}

			return m_ullCount;
		}

		// check if count has been determined for this node
		BOOL
		FCounted() const
		{
			return (EnsCounted == m_ens);
		}

		// number of incoming edges
		ULONG
		UlIncoming() const
		{
			return m_ulIncoming;
		}

		// unrank tree of a given rank with a given rehydrate function
		R *
		PrUnrank(CMemoryPool *mp, PrFn prfn, U *pU, ULLONG ullRank)
		{
			GPOS_CHECK_STACK_SIZE;

			R *pr = NULL;

			if (0 == this->m_ul)
			{
				// global root, just unrank 0-th child
				pr = PrUnrank(mp, prfn, pU, 0 /* ulChild */, ullRank);
			}
			else
			{
				DrgPr *pdrg = GPOS_NEW(mp) DrgPr(mp);

				ULLONG ullRankRem = ullRank;

				ULONG ulChildren = m_pdrgdrgptn->Size();
				for (ULONG ulChild = 0; ulChild < ulChildren; ulChild++)
				{
					ULLONG ullLocalCount = UllCount(ulChild);
					GPOS_ASSERT(0 < ullLocalCount);
					ULLONG ullLocalRank = ullRankRem % ullLocalCount;

					pdrg->Append(PrUnrank(mp, prfn, pU, ulChild, ullLocalRank));

					ullRankRem /= ullLocalCount;
				}

				pr = prfn(mp, const_cast<T *>(this->Value()), pdrg, pU);
			}

			return pr;
		}

#ifdef GPOS_DEBUG

		// debug print
		IOstream &
		OsPrint(IOstream &os)
		{
			ULONG ulChildren = m_pdrgdrgptn->Size();

			os << "=== Node " << m_ul << " [" << *Value()
			   << "] ===" << std::endl
			   << "# children: " << ulChildren << std::endl
			   << "# count: " << this->UllCount() << std::endl;

			for (ULONG ul = 0; ul < ulChildren; ul++)
			{
				os << "--- child: #" << ul << " ---" << std::endl;
				ULONG ulAlt = (*m_pdrgdrgptn)[ul]->Size();

				for (ULONG ulChild = 0; ulChild < ulAlt; ulChild++)
				{
					CTreeNode *ptn = (*(*m_pdrgdrgptn)[ul])[ulChild];
					os << "  -> " << ptn->m_ul << " [" << *ptn->Value() << "]"
					   << std::endl;
				}
			}

			return os;
		}

#endif	// GPOS_DEBUG
	};

	// memory pool
	CMemoryPool *m_mp;

	// counter for nodes
	ULONG m_ulCountNodes;

	// counter for links
	ULONG m_ulCountLinks;

	// rehydrate function
	PrFn m_prfn;

	// universal root (internally used only)
	CTreeNode *m_ptnRoot;

	// map of all nodes
	typedef gpos::CHashMap<T, CTreeNode, HashFn, EqFn, CleanupNULL,
						   CleanupDelete<CTreeNode> >
		TMap;
	typedef gpos::CHashMapIter<T, CTreeNode, HashFn, EqFn, CleanupNULL,
							   CleanupDelete<CTreeNode> >
		TMapIter;

	// map of created links
	typedef CHashMap<STreeLink, BOOL, STreeLink::HashValue, STreeLink::Equals,
					 CleanupDelete<STreeLink>, CleanupDelete<BOOL> >
		LinkMap;

	TMap *m_ptmap;

	// map of nodes to outgoing links
	LinkMap *m_plinkmap;

	// recursive count starting in given node
	ULLONG UllCount(CTreeNode *ptn);

	// Convert to corresponding treenode, create treenode as necessary
	CTreeNode *
	Ptn(const T *value)
	{
		GPOS_ASSERT(NULL != value);
		CTreeNode *ptn = const_cast<CTreeNode *>(m_ptmap->Find(value));

		if (NULL == ptn)
		{
			ptn = GPOS_NEW(m_mp) CTreeNode(m_mp, ++m_ulCountNodes, value);
			(void) m_ptmap->Insert(const_cast<T *>(value), ptn);
		}

		return ptn;
	}

	// private copy ctor
	CTreeMap(const CTreeMap &);

public:
	// ctor
	CTreeMap(CMemoryPool *mp, PrFn prfn)
		: m_mp(mp),
		  m_ulCountNodes(0),
		  m_ulCountLinks(0),
		  m_prfn(prfn),
		  m_ptnRoot(NULL),
		  m_ptmap(NULL),
		  m_plinkmap(NULL)
	{
		GPOS_ASSERT(NULL != mp);
		GPOS_ASSERT(NULL != prfn);

		m_ptmap = GPOS_NEW(mp) TMap(mp);
		m_plinkmap = GPOS_NEW(mp) LinkMap(mp);

		// insert dummy node as global root -- the only node with NULL payload
		m_ptnRoot =
			GPOS_NEW(mp) CTreeNode(mp, 0 /* ulCounter */, NULL /* value */);
	}

	// dtor
	~CTreeMap()
	{
		m_ptmap->Release();
		m_plinkmap->Release();

		GPOS_DELETE(m_ptnRoot);
	}

	// insert edge as n-th child
	void
	Insert(const T *ptParent, ULONG ulPos, const T *ptChild)
	{
		GPOS_ASSERT(ptParent != ptChild);

		// exit function if link already exists
		STreeLink *ptlink = GPOS_NEW(m_mp) STreeLink(ptParent, ulPos, ptChild);
		if (NULL != m_plinkmap->Find(ptlink))
		{
			GPOS_DELETE(ptlink);
			return;
		}

		CTreeNode *ptnParent = Ptn(ptParent);
		CTreeNode *ptnChild = Ptn(ptChild);

		ptnParent->Add(ulPos, ptnChild);
		++m_ulCountLinks;

		// add created link to links map
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif	// GPOS_DEBUG
			m_plinkmap->Insert(ptlink, GPOS_NEW(m_mp) BOOL(true));
		GPOS_ASSERT(fInserted);
	}

	// insert a root node
	void
	InsertRoot(const T *value)
	{
		GPOS_ASSERT(NULL != value);
		GPOS_ASSERT(NULL != m_ptnRoot);

		// add logical root as 0-th child to global root
		m_ptnRoot->Add(0 /*ulPos*/, Ptn(value));
	}

	// count all possible combinations
	ULLONG
	UllCount()
	{
		// first, hookup all logical root nodes to the global root
		TMapIter mi(m_ptmap);
		ULONG ulNodes = 0;
		for (ulNodes = 0; mi.Advance(); ulNodes++)
		{
			CTreeNode *ptn = const_cast<CTreeNode *>(mi.Value());

			if (0 == ptn->UlIncoming())
			{
				// add logical root as 0-th child to global root
				m_ptnRoot->Add(0 /*ulPos*/, ptn);
			}
		}

		// special case of empty map
		if (0 == ulNodes)
		{
			return 0;
		}

		return m_ptnRoot->UllCount();
	}

	// unrank a specific tree
	R *
	PrUnrank(CMemoryPool *mp, U *pU, ULLONG ullRank) const
	{
		return m_ptnRoot->PrUnrank(mp, m_prfn, pU, ullRank);
	}

	// return number of nodes
	ULONG
	UlNodes() const
	{
		return m_ulCountNodes;
	}

	// return number of links
	ULONG
	UlLinks() const
	{
		return m_ulCountLinks;
	}

#ifdef GPOS_DEBUG

	// retrieve count for individual element
	ULLONG
	UllCount(const T *value)
	{
		CTreeNode *ptn = m_ptmap->Find(value);
		GPOS_ASSERT(NULL != ptn);

		return ptn->UllCount();
	}

	// debug print of entire map
	IOstream &
	OsPrint(IOstream &os) const
	{
		TMapIter mi(m_ptmap);
		ULONG ulNodes = 0;
		for (ulNodes = 0; mi.Advance(); ulNodes++)
		{
			CTreeNode *ptn = const_cast<CTreeNode *>(mi.Value());
			(void) ptn->OsPrint(os);
		}

		os << "total number of nodes: " << ulNodes << std::endl;

		return os;
	}

#endif	// GPOS_DEBUG

};	// class CTreeMap

}  // namespace gpopt

#endif
