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
#include <unordered_map>

using namespace gpos;
using namespace duckdb;
using namespace std;

namespace gpopt
{
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
template <class T, class R, class U, ULONG (*HashFn)(const T *), bool (*EqFn)(const T *, const T *)> class CTreeMap
{
	// array of source pointers (sources owned by 3rd party)
	typedef duckdb::vector<T*> DrgPt;

	typedef duckdb::vector<R*> DrgPr;

	// generic rehydrate function
	typedef R* (*PrFn)(T*, DrgPr , U*);

public:
	// fwd declaration
	class CTreeNode;

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
	public:
		// parent node
		const T* m_ptParent;

		// child index
		ULONG m_ulChildIndex;

		// child node
		const T* m_ptChild;

	public:
		// ctor
		STreeLink(const T* ptParent, ULONG child_index, const T* ptChild)
			: m_ptParent(ptParent), m_ulChildIndex(child_index), m_ptChild(ptChild)
		{
		}

		// dtor
		virtual ~STreeLink()
		{
		}

	public:
		// hash function
		ULONG HashValue()
		{
			ULONG ulHashParent = HashFn(m_ptParent);
			ULONG ulHashChild = HashFn(m_ptChild);
			ULONG ulHashChildIndex = gpos::HashValue<ULONG>(&m_ulChildIndex);
			return CombineHashes(ulHashParent, CombineHashes(ulHashChild, ulHashChildIndex));
		}

		// equality function
		static bool Equals(const STreeLink *ptlink1, const STreeLink *ptlink2)
		{
			return EqFn(ptlink1->m_ptParent, ptlink2->m_ptParent) && EqFn(ptlink1->m_ptChild, ptlink2->m_ptChild) && ptlink1->m_ulChildIndex == ptlink2->m_ulChildIndex;
		}

		bool operator==(const STreeLink & ptlink) const
		{
			return EqFn(m_ptParent, ptlink.m_ptParent) && EqFn(m_ptChild, ptlink.m_ptChild) && m_ulChildIndex == ptlink.m_ulChildIndex;
		}
	};	// struct STreeLink

public:
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
			EnsUncounted, EnsCounting, EnsCounted, EnsSentinel
		};

		// id of node
		ULONG m_ul;

		// element
		const T* m_value;

		// array of children arrays
		duckdb::vector<duckdb::vector<shared_ptr<CTreeNode>>> m_pdrgdrgptn;

		// number of trees rooted in this node
		ULLONG m_ullCount;

		// number of incoming edges
		ULONG m_ulIncoming;

		// node state used for counting alternatives
		ENodeState m_ens;

		// total tree count for a given child
		ULLONG UllCount(ULONG ulChild)
		{
			ULLONG ull = 0;
			ULONG ulCandidates = m_pdrgdrgptn[ulChild].size();
			for (ULONG ulAlt = 0; ulAlt < ulCandidates; ulAlt++)
			{
				shared_ptr<CTreeNode> ptn = (m_pdrgdrgptn[ulChild])[ulAlt];
				ULLONG ullCount = ptn->UllCount();
				ull = gpos::Add(ull, ullCount);
			}
			return ull;
		}

		// rehydrate tree
		R* PrUnrank(PrFn prfn, U* pU, ULONG ulChild, ULLONG ullRank)
		{
			duckdb::vector<shared_ptr<CTreeNode>> pdrgptn = m_pdrgdrgptn[ulChild];
			ULONG ulCandidates = pdrgptn.size();
			shared_ptr<CTreeNode> ptn = NULL;
			for (ULONG ul = 0; ul < ulCandidates; ul++)
			{
				ptn = pdrgptn[ul];
				ULLONG ullLocalCount = ptn->UllCount();
				if (ullRank < ullLocalCount)
				{
					// ullRank is now local rank for the child
					break;
				}
				ullRank -= ullLocalCount;
			}
			return ptn->PrUnrank(prfn, pU, ullRank);
		}

	public:
		// ctor
		CTreeNode(ULONG ul, const T* value)
			: m_ul(ul), m_value(value), m_ullCount(gpos::ullong_max), m_ulIncoming(0), m_ens(EnsUncounted)
		{
		}
		
		// dtor
		~CTreeNode()
		{
		}

		// add child alternative
		void Add(ULONG ulPos, CTreeNode* ptn)
		{
			// insert any child arrays skipped so far; make sure we have a dense
			// array up to the position of ulPos
			ULONG length = m_pdrgdrgptn.size();
			for (ULONG ul = length; ul <= ulPos; ul++)
			{
				duckdb::vector<shared_ptr<CTreeNode>> pdrg;
				m_pdrgdrgptn.push_back(pdrg);
			}
			// increment count of incoming edges
			ptn->m_ulIncoming++;
			// insert to appropriate array
			duckdb::vector<shared_ptr<CTreeNode>> pdrg = m_pdrgdrgptn[ulPos];
			pdrg.push_back(shared_ptr<CTreeNode>(ptn));
		}

		// accessor
		const T* Value() const
		{
			return m_value;
		}

		// number of trees rooted in this node
		ULLONG UllCount()
		{
			if (!FCounted())
			{
				// initiate counting on current node
				m_ens = EnsCounting;
				ULLONG ullCount = 1;
				ULONG arity = m_pdrgdrgptn.size();
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
		bool FCounted() const
		{
			return (EnsCounted == m_ens);
		}

		// number of incoming edges
		ULONG UlIncoming() const
		{
			return m_ulIncoming;
		}

		// unrank tree of a given rank with a given rehydrate function
		R* PrUnrank(PrFn prfn, U *pU, ULLONG ullRank)
		{
			R* pr = NULL;
			if (0 == this->m_ul)
			{
				// global root, just unrank 0-th child
				pr = PrUnrank(prfn, pU, 0, ullRank);
			}
			else
			{
				DrgPr pdrg;
				ULLONG ullRankRem = ullRank;
				ULONG ulChildren = m_pdrgdrgptn.size();
				for (ULONG ulChild = 0; ulChild < ulChildren; ulChild++)
				{
					ULLONG ullLocalCount = UllCount(ulChild);
					ULLONG ullLocalRank = ullRankRem % ullLocalCount;
					pdrg.emplace_back(PrUnrank(prfn, pU, ulChild, ullLocalRank));
					ullRankRem /= ullLocalCount;
				}
				pr = prfn(const_cast<T*>(this->Value()), pdrg, pU);
			}
			return pr;
		}
	};

public:
	// counter for nodes
	ULONG m_ulCountNodes;

	// counter for links
	ULONG m_ulCountLinks;

	// rehydrate function
	PrFn m_prfn;

	// universal root (internally used only)
	CTreeNode* m_ptnRoot;

	unordered_map<ULONG, CTreeNode*> m_ptmap;

	// map of nodes to outgoing links
	unordered_map<ULONG, bool> m_plinkmap;

public:
	// ctor
	CTreeMap(PrFn prfn)
		: m_ulCountNodes(0), m_ulCountLinks(0), m_prfn(prfn), m_ptnRoot(NULL)
	{
		// insert dummy node as global root -- the only node with NULL payload
		m_ptnRoot = new CTreeNode(0, NULL);
	}
	
	// no copy ctor
	CTreeMap(const CTreeMap &) = delete;
	
	// dtor
	~CTreeMap()
	{
		m_ptmap.clear();
		m_plinkmap.clear();
		delete m_ptnRoot;
	}

public:
	// recursive count starting in given node
	ULLONG UllCount(CTreeNode* ptn);

	// Convert to corresponding treenode, create treenode as necessary
	CTreeNode* Ptn(const T* value)
	{	
		auto itr = m_ptmap.find(HashFn(value));
		CTreeNode* ptn = const_cast<CTreeNode*>(itr->second);
		if (NULL == ptn)
		{
			ptn = new CTreeNode(++m_ulCountNodes, value);
			m_ptmap.insert(make_pair(HashFn(value), ptn));
		}
		return ptn;
	}

	// insert edge as n-th child
	void Insert(const T* ptParent, ULONG ulPos, const T* ptChild)
	{
		// exit function if link already exists
		STreeLink ptlink(ptParent, ulPos, ptChild);
		if (m_plinkmap.end() != m_plinkmap.find(ptlink.HashValue()))
		{
			return;
		}
		CTreeNode* ptnParent = Ptn(ptParent);
		CTreeNode* ptnChild = Ptn(ptChild);
		ptnParent->Add(ulPos, ptnChild);
		++m_ulCountLinks;
		// add created link to links map
		m_plinkmap.insert(make_pair(ptlink.HashValue(), true));
	}
	
	// insert a root node
	void InsertRoot(const T* value)
	{
		// add logical root as 0-th child to global root
		m_ptnRoot->Add(0, Ptn(value));
	}

	// count all possible combinations
	ULLONG UllCount()
	{
		// first, hookup all logical root nodes to the global root
		for (auto ulNodes = m_ptmap.begin(); ulNodes != m_ptmap.end(); ulNodes++)
		{
			CTreeNode* ptn = const_cast<CTreeNode*>(ulNodes->second);
			if (0 == ptn->UlIncoming())
			{
				// add logical root as 0-th child to global root
				m_ptnRoot->Add(0, ptn);
			}
		}
		// special case of empty map
		if (m_ptmap.size() == 0)
		{
			return 0;
		}
		return m_ptnRoot->UllCount();
	}

	// unrank a specific tree
	R* PrUnrank(U* pU, ULLONG ullRank) const
	{
		return m_ptnRoot->PrUnrank(m_prfn, pU, ullRank);
	}

	// return number of nodes
	ULONG UlNodes() const
	{
		return m_ulCountNodes;
	}

	// return number of links
	ULONG UlLinks() const
	{
		return m_ulCountLinks;
	}
};	// class CTreeMap
}  // namespace gpopt
#endif