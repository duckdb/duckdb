//---------------------------------------------------------------------------
//	@filename:
//		CMDAccessor.h
//
//	@doc:
//		Metadata cache accessor.
//---------------------------------------------------------------------------
#ifndef GPOPT_CMDAccessor_H
#define GPOPT_CMDAccessor_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/memory/CCache.h"
#include "duckdb/optimizer/cascade/memory/CCacheAccessor.h"
#include "duckdb/optimizer/cascade/string/CWStringBase.h"

#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"
#include "duckdb/optimizer/cascade/mdcache/CMDKey.h"
#include "duckdb/optimizer/cascade/md/CSystemId.h"
#include "duckdb/optimizer/cascade/md/IMDFunction.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"
#include "duckdb/optimizer/cascade/md/IMDProvider.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/statistics/IStatistics.h"

namespace gpmd
{
class IMDCacheObject;
class IMDRelation;
class IMDRelationExternal;
class IMDScalarOp;
class IMDAggregate;
class IMDTrigger;
class IMDIndex;
class IMDCheckConstraint;
class IMDProvider;
class CMDProviderGeneric;
class IMDColStats;
class IMDRelStats;
class IMDCast;
class IMDScCmp;
}  // namespace gpmd

namespace gpnaucrates
{
class CHistogram;
class CBucket;
class IStatistics;
}  // namespace gpnaucrates

namespace gpopt
{
using namespace gpos;
using namespace gpmd;


typedef IMDId *MdidPtr;

//---------------------------------------------------------------------------
//	@class:
//		CMDAccessor
//
//	@doc:
//		Gives the optimizer access to metadata information of a particular
//		object (e.g., a Table).
//
//		CMDAccessor maintains a cache of metadata objects (IMDCacheObject)
//		keyed on CMDKey (wrapper over IMDId). It also provides various accessor
//		methods (such as RetrieveAgg(), RetrieveRel() etc.) to request for corresponding
//		metadata objects (such as aggregates and relations respectively). These
//		methods in turn call the private method GetImdObj().
//
//		GetImdObj() first looks up the object in the MDCache. If no information
//		is available in the cache, it goes to a CMDProvider (e.g., GPDB
//		relcache or Minidump) to retrieve the required information.
//
//---------------------------------------------------------------------------
class CMDAccessor
{
public:
	// ccache template for mdcache
	typedef CCache<IMDCacheObject *, CMDKey *> MDCache;

private:
	// element in the hashtable of cache accessors maintained by the MD accessor
	struct SMDAccessorElem;
	struct SMDProviderElem;


	// cache accessor for objects in a MD cache
	typedef CCacheAccessor<IMDCacheObject *, CMDKey *> CacheAccessorMD;

	// hashtable for cache accessors indexed by the md id of the accessed object
	typedef CSyncHashtable<SMDAccessorElem, MdidPtr> MDHT;

	typedef CSyncHashtableAccessByKey<SMDAccessorElem, MdidPtr> MDHTAccessor;

	// iterator for the cache accessors hashtable
	typedef CSyncHashtableIter<SMDAccessorElem, MdidPtr> MDHTIter;
	typedef CSyncHashtableAccessByIter<SMDAccessorElem, MdidPtr>
		MDHTIterAccessor;

	// hashtable for MD providers indexed by the source system id
	typedef CSyncHashtable<SMDProviderElem, SMDProviderElem> MDPHT;

	typedef CSyncHashtableAccessByKey<SMDProviderElem, SMDProviderElem>
		MDPHTAccessor;

	// iterator for the providers hashtable
	typedef CSyncHashtableIter<SMDProviderElem, SMDProviderElem> MDPHTIter;
	typedef CSyncHashtableAccessByIter<SMDProviderElem, SMDProviderElem>
		MDPHTIterAccessor;

	// element in the cache accessor hashtable maintained by the MD Accessor
	struct SMDAccessorElem
	{
	private:
		// hashed object
		IMDCacheObject *m_imd_obj;

	public:
		// hash key
		IMDId *m_mdid;

		// generic link
		SLink m_link;

		// invalid key
		static const MdidPtr m_pmdidInvalid;

		// ctor
		SMDAccessorElem(IMDCacheObject *pimdobj, IMDId *mdid);

		// dtor
		~SMDAccessorElem();

		// hashed object
		IMDCacheObject *
		GetImdObj()
		{
			return m_imd_obj;
		}

		// return the key for this hashtable element
		IMDId *MDId();

		// equality function for hash tables
		static BOOL Equals(const MdidPtr &left_mdid, const MdidPtr &right_mdid);

		// hash function for cost contexts hash table
		static ULONG HashValue(const MdidPtr &mdid);
	};

	// element in the MD provider hashtable
	struct SMDProviderElem
	{
	private:
		// source system id
		CSystemId m_sysid;

		// value of the hashed element
		IMDProvider *m_pmdp;

	public:
		// generic link
		SLink m_link;

		// invalid key
		static const SMDProviderElem m_mdpelemInvalid;

		// ctor
		SMDProviderElem(CSystemId sysid, IMDProvider *pmdp);

		// dtor
		~SMDProviderElem();

		// return the MD provider
		IMDProvider *Pmdp();

		// return the system id
		CSystemId Sysid() const;

		// equality function for hash tables
		static BOOL Equals(const SMDProviderElem &mdpelemLeft,
						   const SMDProviderElem &mdpelemRight);

		// hash function for MD providers hash table
		static ULONG HashValue(const SMDProviderElem &mdpelem);
	};

private:
	// memory pool
	CMemoryPool *m_mp;

	// metadata cache
	MDCache *m_pcache;

	// generic metadata provider
	CMDProviderGeneric *m_pmdpGeneric;

	// hashtable of cache accessors
	MDHT m_shtCacheAccessors;

	// hashtable of MD providers
	MDPHT m_shtProviders;

	// total time consumed in looking up MD objects (including time used to fetch objects from MD provider)
	CDouble m_dLookupTime;

	// total time consumed in fetching MD objects from MD provider,
	// this time is currently dominated by serialization time
	CDouble m_dFetchTime;

	// private copy ctor
	CMDAccessor(const CMDAccessor &);

	// interface to a MD cache object
	const IMDCacheObject *GetImdObj(IMDId *mdid);

	// return the type corresponding to the given type info and source system id
	const IMDType *RetrieveType(CSystemId sysid, IMDType::ETypeInfo type_info);

	// return the generic type corresponding to the given type info
	const IMDType *RetrieveType(IMDType::ETypeInfo type_info);

	// destroy accessor element when MDAccessor is destroyed
	static void DestroyAccessorElement(SMDAccessorElem *pmdaccelem);

	// destroy accessor element when MDAccessor is destroyed
	static void DestroyProviderElement(SMDProviderElem *pmdpelem);

	// lookup an MD provider by system id
	IMDProvider *Pmdp(CSystemId sysid);

	// initialize hash tables
	void InitHashtables(CMemoryPool *mp);

	// return the column statistics meta data object for a given column of a table
	const IMDColStats *Pmdcolstats(CMemoryPool *mp, IMDId *rel_mdid,
								   ULONG ulPos);

	// record histogram and width information for a given column of a table
	void RecordColumnStats(CMemoryPool *mp, IMDId *rel_mdid, ULONG colid,
						   ULONG ulPos, BOOL fSystemCol, BOOL fEmptyTable,
						   UlongToHistogramMap *col_histogram_mapping,
						   UlongToDoubleMap *colid_width_mapping,
						   CStatisticsConfig *stats_config);

	// construct a stats histogram from an MD column stats object
	CHistogram *GetHistogram(CMemoryPool *mp, IMDId *mdid_type,
							 const IMDColStats *pmdcolstats);

public:
	// ctors
	CMDAccessor(CMemoryPool *mp, MDCache *pcache);
	CMDAccessor(CMemoryPool *mp, MDCache *pcache, CSystemId sysid,
				IMDProvider *pmdp);
	CMDAccessor(CMemoryPool *mp, MDCache *pcache,
				const CSystemIdArray *pdrgpsysid,
				const CMDProviderArray *pdrgpmdp);

	//dtor
	~CMDAccessor();

	// return MD cache
	MDCache *
	Pcache() const
	{
		return m_pcache;
	}

	// register a new MD provider
	void RegisterProvider(CSystemId sysid, IMDProvider *pmdp);

	// register given MD providers
	void RegisterProviders(const CSystemIdArray *pdrgpsysid,
						   const CMDProviderArray *pdrgpmdp);

	// interface to a relation object from the MD cache
	const IMDRelation *RetrieveRel(IMDId *mdid);

	// interface to type's from the MD cache given the type's mdid
	const IMDType *RetrieveType(IMDId *mdid);

	// obtain the specified base type given by the template parameter
	template <class T>
	const T *
	PtMDType()
	{
		IMDType::ETypeInfo type_info = T::GetTypeInfo();
		GPOS_ASSERT(IMDType::EtiGeneric != type_info);
		return dynamic_cast<const T *>(RetrieveType(type_info));
	}

	// obtain the specified base type given by the template parameter
	template <class T>
	const T *
	PtMDType(CSystemId sysid)
	{
		IMDType::ETypeInfo type_info = T::GetTypeInfo();
		GPOS_ASSERT(IMDType::EtiGeneric != type_info);
		return dynamic_cast<const T *>(RetrieveType(sysid, type_info));
	}

	// interface to a scalar operator from the MD cache
	const IMDScalarOp *RetrieveScOp(IMDId *mdid);

	// interface to a function from the MD cache
	const IMDFunction *RetrieveFunc(IMDId *mdid);

	// interface to check if the window function from the MD cache is an aggregate window function
	BOOL FAggWindowFunc(IMDId *mdid);

	// interface to an aggregate from the MD cache
	const IMDAggregate *RetrieveAgg(IMDId *mdid);

	// interface to a trigger from the MD cache
	const IMDTrigger *RetrieveTrigger(IMDId *mdid);

	// interface to an index from the MD cache
	const IMDIndex *RetrieveIndex(IMDId *mdid);

	// interface to a check constraint from the MD cache
	const IMDCheckConstraint *RetrieveCheckConstraints(IMDId *mdid);

	// retrieve a column stats object from the cache
	const IMDColStats *Pmdcolstats(IMDId *mdid);

	// retrieve a relation stats object from the cache
	const IMDRelStats *Pmdrelstats(IMDId *mdid);

	// retrieve a cast object from the cache
	const IMDCast *Pmdcast(IMDId *mdid_src, IMDId *mdid_dest);

	// retrieve a scalar comparison object from the cache
	const IMDScCmp *Pmdsccmp(IMDId *left_mdid, IMDId *right_mdid,
							 IMDType::ECmpType cmp_type);

	// construct a statistics object for the columns of the given relation
	IStatistics *Pstats(
		CMemoryPool *mp, IMDId *rel_mdid,
		CColRefSet
			*pcrsHist,	// set of column references for which stats are needed
		CColRefSet *
			pcrsWidth,	// set of column references for which the widths are needed
		CStatisticsConfig *stats_config = NULL);

	// serialize object to passed stream
	void Serialize(COstream &oos);

	// serialize system ids to passed stream
	void SerializeSysid(COstream &oos);
};
}  // namespace gpopt



#endif	// !GPOPT_CMDAccessor_H

// EOF
