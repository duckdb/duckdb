//---------------------------------------------------------------------------
//	@filename:
//		CMDAccessor.cpp
//
//	@doc:
//		Implementation of the metadata accessor class handling accesses to
//		metadata objects in an optimization session
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/common/CAutoP.h"
#include "duckdb/optimizer/cascade/common/CAutoRef.h"
#include "duckdb/optimizer/cascade/common/CTimerUser.h"
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/task/CAutoSuspendAbort.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessorUtils.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/md/CMDIdCast.h"
#include "duckdb/optimizer/cascade/md/CMDIdColStats.h"
#include "duckdb/optimizer/cascade/md/CMDIdRelStats.h"
#include "duckdb/optimizer/cascade/md/CMDIdScCmp.h"
#include "duckdb/optimizer/cascade/md/CMDProviderGeneric.h"
#include "duckdb/optimizer/cascade/md/IMDAggregate.h"
#include "duckdb/optimizer/cascade/md/IMDCacheObject.h"
#include "duckdb/optimizer/cascade/md/IMDCast.h"
#include "duckdb/optimizer/cascade/md/IMDCheckConstraint.h"
#include "duckdb/optimizer/cascade/md/IMDColStats.h"
#include "duckdb/optimizer/cascade/md/IMDFunction.h"
#include "duckdb/optimizer/cascade/md/IMDIndex.h"
#include "duckdb/optimizer/cascade/md/IMDProvider.h"
#include "duckdb/optimizer/cascade/md/IMDRelStats.h"
#include "duckdb/optimizer/cascade/md/IMDRelation.h"
#include "duckdb/optimizer/cascade/md/IMDRelationExternal.h"
#include "duckdb/optimizer/cascade/md/IMDScCmp.h"
#include "duckdb/optimizer/cascade/md/IMDScalarOp.h"
#include "duckdb/optimizer/cascade/md/IMDTrigger.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"

using namespace gpos;
using namespace gpmd;
using namespace gpopt;

// no. of hashtable buckets
#define GPOPT_CACHEACC_HT_NUM_OF_BUCKETS 128

// static member initialization

// invalid mdid pointer
const MdidPtr CMDAccessor::SMDAccessorElem::m_pmdidInvalid = NULL;

// invalid md provider element
const CMDAccessor::SMDProviderElem
	CMDAccessor::SMDProviderElem::m_mdpelemInvalid(
		CSystemId(IMDId::EmdidSentinel, NULL, 0), NULL);

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::SMDAccessorElem
//
//	@doc:
//		Constructs a metadata accessor element for the accessors hashtable
//
//---------------------------------------------------------------------------
CMDAccessor::SMDAccessorElem::SMDAccessorElem(IMDCacheObject *pimdobj,
											  IMDId *mdid)
	: m_imd_obj(pimdobj), m_mdid(mdid)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::~SMDAccessorElem
//
//	@doc:
//		Destructor for the metadata accessor element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDAccessorElem::~SMDAccessorElem()
{
	// deleting the cache accessor will effectively unpin the cache entry for that object
	m_imd_obj->Release();
	m_mdid->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::MDId
//
//	@doc:
//		Return the key for this hashtable element
//
//---------------------------------------------------------------------------
IMDId *
CMDAccessor::SMDAccessorElem::MDId()
{
	return m_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::Equals
//
//	@doc:
//		Equality function for cache accessors hash table
//
//---------------------------------------------------------------------------
BOOL
CMDAccessor::SMDAccessorElem::Equals(const MdidPtr &left_mdid,
									 const MdidPtr &right_mdid)
{
	if (left_mdid == m_pmdidInvalid || right_mdid == m_pmdidInvalid)
	{
		return left_mdid == m_pmdidInvalid && right_mdid == m_pmdidInvalid;
	}

	return left_mdid->Equals(right_mdid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDAccessorElem::HashValue
//
//	@doc:
//		Hash function for cache accessors hash table
//
//---------------------------------------------------------------------------
ULONG
CMDAccessor::SMDAccessorElem::HashValue(const MdidPtr &mdid)
{
	GPOS_ASSERT(m_pmdidInvalid != mdid);

	return mdid->HashValue();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::SMDProviderElem
//
//	@doc:
//		Constructs an MD provider element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDProviderElem::SMDProviderElem(CSystemId sysid,
											  IMDProvider *pmdp)
	: m_sysid(sysid), m_pmdp(pmdp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::~SMDProviderElem
//
//	@doc:
//		Destructor for the MD provider element
//
//---------------------------------------------------------------------------
CMDAccessor::SMDProviderElem::~SMDProviderElem()
{
	CRefCount::SafeRelease(m_pmdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::Pmdp
//
//	@doc:
//		Returns the MD provider for this hash table element
//
//---------------------------------------------------------------------------
IMDProvider *
CMDAccessor::SMDProviderElem::Pmdp()
{
	return m_pmdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::Sysid
//
//	@doc:
//		Returns the system id for this hash table element
//
//---------------------------------------------------------------------------
CSystemId
CMDAccessor::SMDProviderElem::Sysid() const
{
	return m_sysid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::Equals
//
//	@doc:
//		Equality function for hash tables
//
//---------------------------------------------------------------------------
BOOL
CMDAccessor::SMDProviderElem::Equals(const SMDProviderElem &mdpelemLeft,
									 const SMDProviderElem &mdpelemRight)
{
	return mdpelemLeft.m_sysid.Equals(mdpelemRight.m_sysid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SMDProviderElem::HashValue
//
//	@doc:
//		Hash function for cost contexts hash table
//
//---------------------------------------------------------------------------
ULONG
CMDAccessor::SMDProviderElem::HashValue(const SMDProviderElem &mdpelem)
{
	GPOS_ASSERT(!Equals(mdpelem, m_mdpelemInvalid));

	return mdpelem.m_sysid.HashValue();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor(CMemoryPool *mp, MDCache *pcache)
	: m_mp(mp), m_pcache(pcache), m_dLookupTime(0.0), m_dFetchTime(0.0)
{
	GPOS_ASSERT(NULL != m_mp);
	GPOS_ASSERT(NULL != m_pcache);

	m_pmdpGeneric = GPOS_NEW(mp) CMDProviderGeneric(mp);

	InitHashtables(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor and registers an MD provider
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor(CMemoryPool *mp, MDCache *pcache, CSystemId sysid,
						 IMDProvider *pmdp)
	: m_mp(mp), m_pcache(pcache), m_dLookupTime(0.0), m_dFetchTime(0.0)
{
	GPOS_ASSERT(NULL != m_mp);
	GPOS_ASSERT(NULL != m_pcache);

	m_pmdpGeneric = GPOS_NEW(mp) CMDProviderGeneric(mp);

	InitHashtables(mp);

	RegisterProvider(sysid, pmdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::CMDAccessor
//
//	@doc:
//		Constructs a metadata accessor and registers MD providers
//
//---------------------------------------------------------------------------
CMDAccessor::CMDAccessor(CMemoryPool *mp, MDCache *pcache,
						 const CSystemIdArray *pdrgpsysid,
						 const CMDProviderArray *pdrgpmdp)
	: m_mp(mp), m_pcache(pcache), m_dLookupTime(0.0), m_dFetchTime(0.0)
{
	GPOS_ASSERT(NULL != m_mp);
	GPOS_ASSERT(NULL != m_pcache);

	m_pmdpGeneric = GPOS_NEW(mp) CMDProviderGeneric(mp);

	InitHashtables(mp);

	RegisterProviders(pdrgpsysid, pdrgpmdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::DestroyAccessorElement
//
//	@doc:
//		Destroy accessor element;
//		called only at destruction time
//
//---------------------------------------------------------------------------
void
CMDAccessor::DestroyAccessorElement(SMDAccessorElem *pmdaccelem)
{
	GPOS_ASSERT(NULL != pmdaccelem);

	// remove deletion lock for mdid
	pmdaccelem->MDId()->RemoveDeletionLock();
	;

	GPOS_DELETE(pmdaccelem);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::DestroyProviderElement
//
//	@doc:
//		Destroy provider element;
//		called only at destruction time
//
//---------------------------------------------------------------------------
void
CMDAccessor::DestroyProviderElement(SMDProviderElem *pmdpelem)
{
	GPOS_DELETE(pmdpelem);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::InitHashtables
//
//	@doc:
//		Initializes the hash tables
//
//---------------------------------------------------------------------------
void
CMDAccessor::InitHashtables(CMemoryPool *mp)
{
	// initialize Cache accessors hash table
	m_shtCacheAccessors.Init(mp, GPOPT_CACHEACC_HT_NUM_OF_BUCKETS,
							 GPOS_OFFSET(SMDAccessorElem, m_link),
							 GPOS_OFFSET(SMDAccessorElem, m_mdid),
							 &(SMDAccessorElem::m_pmdidInvalid),
							 SMDAccessorElem::HashValue,
							 SMDAccessorElem::Equals);

	// initialize MD providers hash table
	m_shtProviders.Init(mp, GPOPT_CACHEACC_HT_NUM_OF_BUCKETS,
						GPOS_OFFSET(SMDProviderElem, m_link),
						0,	// the HT element is used as key
						&(SMDProviderElem::m_mdpelemInvalid),
						SMDProviderElem::HashValue, SMDProviderElem::Equals);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::~CMDAccessor
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CMDAccessor::~CMDAccessor()
{
	// release cache accessors and MD providers in hashtables
	m_shtCacheAccessors.DestroyEntries(DestroyAccessorElement);
	m_shtProviders.DestroyEntries(DestroyProviderElement);
	GPOS_DELETE(m_pmdpGeneric);

	if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
	{
		// print fetch time and lookup time
		CAutoTrace at(m_mp);
		at.Os() << "[OPT]: Total metadata fetch time: " << m_dFetchTime << "ms"
				<< std::endl;
		at.Os() << "[OPT]: Total metadata lookup time (including fetch time): "
				<< m_dLookupTime << "ms" << std::endl;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RegisterProvider
//
//	@doc:
//		Register a MD provider for the given source system id
//
//---------------------------------------------------------------------------
void
CMDAccessor::RegisterProvider(CSystemId sysid, IMDProvider *pmdp)
{
	CAutoP<SMDProviderElem> a_pmdpelem;
	a_pmdpelem = GPOS_NEW(m_mp) SMDProviderElem(sysid, pmdp);

	MDPHTAccessor mdhtacc(m_shtProviders, *(a_pmdpelem.Value()));

	GPOS_ASSERT(NULL == mdhtacc.Find());

	// insert provider in the hash table
	mdhtacc.Insert(a_pmdpelem.Value());
	a_pmdpelem.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RegisterProviders
//
//	@doc:
//		Register given MD providers
//
//---------------------------------------------------------------------------
void
CMDAccessor::RegisterProviders(const CSystemIdArray *pdrgpsysid,
							   const CMDProviderArray *pdrgpmdp)
{
	GPOS_ASSERT(NULL != pdrgpmdp);
	GPOS_ASSERT(NULL != pdrgpsysid);
	GPOS_ASSERT(pdrgpmdp->Size() == pdrgpsysid->Size());
	GPOS_ASSERT(0 < pdrgpmdp->Size());

	const ULONG ulProviders = pdrgpmdp->Size();
	for (ULONG ul = 0; ul < ulProviders; ul++)
	{
		IMDProvider *pmdp = (*pdrgpmdp)[ul];
		pmdp->AddRef();
		RegisterProvider(*((*pdrgpsysid)[ul]), pmdp);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdp
//
//	@doc:
//		Retrieve the MD provider for the given source system id
//
//---------------------------------------------------------------------------
IMDProvider *
CMDAccessor::Pmdp(CSystemId sysid)
{
	SMDProviderElem *pmdpelem = NULL;

	{
		// scope for HT accessor

		SMDProviderElem mdpelem(sysid, NULL /*pmdp*/);
		MDPHTAccessor mdhtacc(m_shtProviders, mdpelem);

		pmdpelem = mdhtacc.Find();
	}

	GPOS_ASSERT(NULL != pmdpelem && "Could not find MD provider");

	return pmdpelem->Pmdp();
}



//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::GetImdObj
//
//	@doc:
//		Retrieves a metadata cache object from the md cache, possibly retrieving
//		it from the external metadata provider and storing it in the cache first.
//		Main workhorse for retrieving the different types of md cache objects.
//
//---------------------------------------------------------------------------
const IMDCacheObject *
CMDAccessor::GetImdObj(IMDId *mdid)
{
	BOOL fPrintOptStats = GPOS_FTRACE(EopttracePrintOptimizationStatistics);
	CTimerUser timerLookup;	 // timer to measure lookup time
	if (fPrintOptStats)
	{
		timerLookup.Restart();
	}

	const IMDCacheObject *pimdobj = NULL;

	// first, try to locate object in local hashtable
	{
		// scope for ht accessor
		MDHTAccessor mdhtacc(m_shtCacheAccessors, mdid);
		SMDAccessorElem *pmdaccelem = mdhtacc.Find();
		if (NULL != pmdaccelem)
		{
			pimdobj = pmdaccelem->GetImdObj();
		}
	}

	if (NULL == pimdobj)
	{
		// object not in local hashtable, try lookup in the MD cache

		// construct a key for cache lookup
		IMDProvider *pmdp = Pmdp(mdid->Sysid());

		CMDKey mdkey(mdid);

		CAutoP<CacheAccessorMD> a_pmdcacc;
		a_pmdcacc = GPOS_NEW(m_mp) CacheAccessorMD(m_pcache);
		a_pmdcacc->Lookup(&mdkey);
		IMDCacheObject *pmdobjNew = a_pmdcacc->Val();
		{
			// store in local hashtable
			GPOS_ASSERT(NULL != pmdobjNew);
			IMDId *pmdidNew = pmdobjNew->MDId();
			pmdidNew->AddRef();

			CAutoP<SMDAccessorElem> a_pmdaccelem;
			a_pmdaccelem = GPOS_NEW(m_mp) SMDAccessorElem(pmdobjNew, pmdidNew);

			MDHTAccessor mdhtacc(m_shtCacheAccessors, pmdidNew);

			if (NULL == mdhtacc.Find())
			{
				// object has not been inserted in the meantime
				mdhtacc.Insert(a_pmdaccelem.Value());

				// add deletion lock for mdid
				pmdidNew->AddDeletionLock();
				a_pmdaccelem.Reset();
			}
		}
	}

	// requested object must be in local hashtable already: retrieve it
	MDHTAccessor mdhtacc(m_shtCacheAccessors, mdid);
	SMDAccessorElem *pmdaccelem = mdhtacc.Find();

	GPOS_ASSERT(NULL != pmdaccelem);

	pimdobj = pmdaccelem->GetImdObj();
	GPOS_ASSERT(NULL != pimdobj);

	if (fPrintOptStats)
	{
		// add lookup time in msec
		CDouble dLookup(timerLookup.ElapsedUS() / CDouble(GPOS_USEC_IN_MSEC));
		m_dLookupTime = CDouble(m_dLookupTime.Get() + dLookup.Get());
	}

	return pimdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveRel
//
//	@doc:
//		Retrieves a metadata cache relation from the md cache, possibly retrieving
//		it from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDRelation *
CMDAccessor::RetrieveRel(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtRel != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDRelation *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveType
//
//	@doc:
//		Retrieves the metadata description for a type from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDType* CMDAccessor::RetrieveType(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtType != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDType *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveType
//
//	@doc:
//		Retrieves the MD type from the md cache given the type info and source
//		system id,  possibly retrieving it from the external metadata provider
//		and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDType* CMDAccessor::RetrieveType(CSystemId sysid, IMDType::ETypeInfo type_info)
{
	GPOS_ASSERT(IMDType::EtiGeneric != type_info);
	IMDProvider *pmdp = Pmdp(sysid);
	CAutoRef<IMDId> a_pmdid;
	a_pmdid = pmdp->MDId(m_mp, sysid, type_info);
	const IMDCacheObject *pmdobj = GetImdObj(a_pmdid.Value());
	if (IMDCacheObject::EmdtType != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, a_pmdid.Value()->GetBuffer());
	}
	return dynamic_cast<const IMDType *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveType
//
//	@doc:
//		Retrieves the generic MD type from the md cache given the
//		type info,  possibly retrieving it from the external metadata provider
//		and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDType* CMDAccessor::RetrieveType(IMDType::ETypeInfo type_info)
{
	GPOS_ASSERT(IMDType::EtiGeneric != type_info);
	IMDId *mdid = m_pmdpGeneric->MDId(type_info);
	GPOS_ASSERT(NULL != mdid);
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtType != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDType *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveScOp
//
//	@doc:
//		Retrieves the metadata description for a scalar operator from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDScalarOp* CMDAccessor::RetrieveScOp(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtOp != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDScalarOp *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveFunc
//
//	@doc:
//		Retrieves the metadata description for a function from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDFunction* CMDAccessor::RetrieveFunc(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtFunc != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDFunction *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::FaggWindowFunc
//
//	@doc:
//		Check if the retrieved the window function metadata description from
//		the md cache is an aggregate window function. Internally this function
//		may retrieve it from the external metadata provider and storing
//		it in the cache.
//
//---------------------------------------------------------------------------
BOOL CMDAccessor::FAggWindowFunc(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	return (IMDCacheObject::EmdtAgg == pmdobj->MDType());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveAgg
//
//	@doc:
//		Retrieves the metadata description for an aggregate from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDAggregate* CMDAccessor::RetrieveAgg(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtAgg != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDAggregate *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveTrigger
//
//	@doc:
//		Retrieves the metadata description for a trigger from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDTrigger* CMDAccessor::RetrieveTrigger(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtTrigger != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDTrigger *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveIndex
//
//	@doc:
//		Retrieves the metadata description for an index from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDIndex* CMDAccessor::RetrieveIndex(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtInd != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDIndex *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::RetrieveCheckConstraints
//
//	@doc:
//		Retrieves the metadata description for a check constraint from the md cache,
//		possibly retrieving it from the external metadata provider and storing
//		it in the cache first.
//
//---------------------------------------------------------------------------
const IMDCheckConstraint* CMDAccessor::RetrieveCheckConstraints(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtCheckConstraint != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDCheckConstraint *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdcolstats
//
//	@doc:
//		Retrieves column statistics from the md cache, possibly retrieving it
//		from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDColStats* CMDAccessor::Pmdcolstats(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtColStats != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}

	return dynamic_cast<const IMDColStats *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdrelstats
//
//	@doc:
//		Retrieves relation statistics from the md cache, possibly retrieving it
//		from the external metadata provider and storing it in the cache first.
//
//---------------------------------------------------------------------------
const IMDRelStats* CMDAccessor::Pmdrelstats(IMDId *mdid)
{
	const IMDCacheObject *pmdobj = GetImdObj(mdid);
	if (IMDCacheObject::EmdtRelStats != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, mdid->GetBuffer());
	}
	return dynamic_cast<const IMDRelStats *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdcast
//
//	@doc:
//		Retrieve cast object between given source and destination types
//
//---------------------------------------------------------------------------
const IMDCast* CMDAccessor::Pmdcast(IMDId *mdid_src, IMDId *mdid_dest)
{
	GPOS_ASSERT(NULL != mdid_src);
	GPOS_ASSERT(NULL != mdid_dest);
	mdid_src->AddRef();
	mdid_dest->AddRef();
	CAutoP<IMDId> a_pmdidCast;
	a_pmdidCast = GPOS_NEW(m_mp) CMDIdCast(CMDIdGPDB::CastMdid(mdid_src), CMDIdGPDB::CastMdid(mdid_dest));
	const IMDCacheObject *pmdobj = GetImdObj(a_pmdidCast.Value());
	if (IMDCacheObject::EmdtCastFunc != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, a_pmdidCast->GetBuffer());
	}
	a_pmdidCast.Reset()->Release();
	return dynamic_cast<const IMDCast *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pmdsccmp
//
//	@doc:
//		Retrieve scalar comparison object between given types
//
//---------------------------------------------------------------------------
const IMDScCmp* CMDAccessor::Pmdsccmp(IMDId *left_mdid, IMDId *right_mdid, IMDType::ECmpType cmp_type)
{
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(NULL != left_mdid);
	GPOS_ASSERT(IMDType::EcmptOther > cmp_type);
	left_mdid->AddRef();
	right_mdid->AddRef();
	CAutoP<IMDId> a_pmdidScCmp;
	a_pmdidScCmp = GPOS_NEW(m_mp) CMDIdScCmp(CMDIdGPDB::CastMdid(left_mdid), CMDIdGPDB::CastMdid(right_mdid), cmp_type);
	const IMDCacheObject *pmdobj = GetImdObj(a_pmdidScCmp.Value());
	if (IMDCacheObject::EmdtScCmp != pmdobj->MDType())
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiNoPlanFound, a_pmdidScCmp->GetBuffer());
	}
	a_pmdidScCmp.Reset()->Release();
	return dynamic_cast<const IMDScCmp *>(pmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::ExtractColumnHistWidth
//
//	@doc:
//		Record histogram and width information for a given column of a table
//
//---------------------------------------------------------------------------
void CMDAccessor::RecordColumnStats(CMemoryPool *mp, IMDId *rel_mdid, ULONG colid, ULONG ulPos, BOOL fSystemCol, BOOL fEmptyTable, UlongToHistogramMap *col_histogram_mapping, UlongToDoubleMap *colid_width_mapping, CStatisticsConfig *stats_config)
{
	GPOS_ASSERT(NULL != rel_mdid);
	GPOS_ASSERT(NULL != col_histogram_mapping);
	GPOS_ASSERT(NULL != colid_width_mapping);

	// get the column statistics
	const IMDColStats *pmdcolstats = Pmdcolstats(mp, rel_mdid, ulPos);
	GPOS_ASSERT(NULL != pmdcolstats);

	// fetch the column width and insert it into the hashmap
	CDouble *width = GPOS_NEW(mp) CDouble(pmdcolstats->Width());
	colid_width_mapping->Insert(GPOS_NEW(mp) ULONG(colid), width);

	// extract the the histogram and insert it into the hashmap
	const IMDRelation *pmdrel = RetrieveRel(rel_mdid);
	IMDId *mdid_type = pmdrel->GetMdCol(ulPos)->MdidType();
	CHistogram *histogram = GetHistogram(mp, mdid_type, pmdcolstats);
	GPOS_ASSERT(NULL != histogram);
	col_histogram_mapping->Insert(GPOS_NEW(mp) ULONG(colid), histogram);

	BOOL fGuc = GPOS_FTRACE(EopttracePrintColsWithMissingStats);
	BOOL fRecordMissingStats = !fEmptyTable && fGuc && !fSystemCol &&
							   (NULL != stats_config) &&
							   histogram->IsColStatsMissing();
	if (fRecordMissingStats)
	{
		// record the columns with missing (dummy) statistics information
		rel_mdid->AddRef();
		CMDIdColStats *pmdidCol = GPOS_NEW(mp) CMDIdColStats(CMDIdGPDB::CastMdid(rel_mdid), ulPos);
		stats_config->AddMissingStatsColumn(pmdidCol);
		pmdidCol->Release();
	}
}


// Return the column statistics meta data object for a given column of a table
const IMDColStats *
CMDAccessor::Pmdcolstats(CMemoryPool *mp, IMDId *rel_mdid, ULONG ulPos)
{
	rel_mdid->AddRef();
	CMDIdColStats *mdid_col_stats =
		GPOS_NEW(mp) CMDIdColStats(CMDIdGPDB::CastMdid(rel_mdid), ulPos);
	const IMDColStats *pmdcolstats = Pmdcolstats(mdid_col_stats);
	mdid_col_stats->Release();

	return pmdcolstats;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pstats
//
//	@doc:
//		Construct a statistics object for the columns of the given relation
//
//---------------------------------------------------------------------------
IStatistics *
CMDAccessor::Pstats(CMemoryPool *mp, IMDId *rel_mdid, CColRefSet *pcrsHist,
					CColRefSet *pcrsWidth, CStatisticsConfig *stats_config)
{
	GPOS_ASSERT(NULL != rel_mdid);
	GPOS_ASSERT(NULL != pcrsHist);
	GPOS_ASSERT(NULL != pcrsWidth);

	// retrieve MD relation and MD relation stats objects
	rel_mdid->AddRef();
	CMDIdRelStats *rel_stats_mdid =
		GPOS_NEW(mp) CMDIdRelStats(CMDIdGPDB::CastMdid(rel_mdid));
	const IMDRelStats *pmdRelStats = Pmdrelstats(rel_stats_mdid);
	rel_stats_mdid->Release();

	BOOL fEmptyTable = pmdRelStats->IsEmpty();
	const IMDRelation *pmdrel = RetrieveRel(rel_mdid);

	UlongToHistogramMap *col_histogram_mapping =
		GPOS_NEW(mp) UlongToHistogramMap(mp);
	UlongToDoubleMap *colid_width_mapping = GPOS_NEW(mp) UlongToDoubleMap(mp);

	CColRefSetIter crsiHist(*pcrsHist);
	while (crsiHist.Advance())
	{
		CColRef *pcrHist = crsiHist.Pcr();

		// colref must be one of the base table
		CColRefTable *pcrtable = CColRefTable::PcrConvert(pcrHist);

		// extract the column identifier, position of the attribute in the system catalog
		ULONG colid = pcrtable->Id();
		INT attno = pcrtable->AttrNum();
		ULONG ulPos = pmdrel->GetPosFromAttno(attno);

		RecordColumnStats(mp, rel_mdid, colid, ulPos, pcrtable->FSystemCol(),
						  fEmptyTable, col_histogram_mapping,
						  colid_width_mapping, stats_config);
	}

	// extract column widths
	CColRefSetIter crsiWidth(*pcrsWidth);

	while (crsiWidth.Advance())
	{
		CColRef *pcrWidth = crsiWidth.Pcr();

		// colref must be one of the base table
		CColRefTable *pcrtable = CColRefTable::PcrConvert(pcrWidth);

		// extract the column identifier, position of the attribute in the system catalog
		ULONG colid = pcrtable->Id();
		INT attno = pcrtable->AttrNum();
		ULONG ulPos = pmdrel->GetPosFromAttno(attno);

		CDouble *width = GPOS_NEW(mp) CDouble(pmdrel->ColWidth(ulPos));
		colid_width_mapping->Insert(GPOS_NEW(mp) ULONG(colid), width);
	}

	CDouble rows = std::max(DOUBLE(1.0), pmdRelStats->Rows().Get());

	return GPOS_NEW(mp) CStatistics(mp, col_histogram_mapping,
									colid_width_mapping, rows, fEmptyTable);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::GetHistogram
//
//	@doc:
//		Construct a histogram from the given MD column stats object
//
//---------------------------------------------------------------------------
CHistogram* CMDAccessor::GetHistogram(CMemoryPool *mp, IMDId *mdid_type, const IMDColStats *pmdcolstats)
{
	GPOS_ASSERT(NULL != mdid_type);
	GPOS_ASSERT(NULL != pmdcolstats);
	BOOL is_col_stats_missing = pmdcolstats->IsColStatsMissing();
	const ULONG num_of_buckets = pmdcolstats->Buckets();
	BOOL fBoolType = CMDAccessorUtils::FBoolType(this, mdid_type);
	if (is_col_stats_missing && fBoolType)
	{
		GPOS_ASSERT(0 == num_of_buckets);

		return CHistogram::MakeDefaultBoolHistogram(mp);
	}
	CBucketArray *buckets = GPOS_NEW(mp) CBucketArray(mp);
	//for (ULONG ul = 0; ul < num_of_buckets; ul++)
	//{
	//	const CDXLBucket *dxl_bucket = pmdcolstats->GetDXLBucketAt(ul);
	//	CBucket *bucket = Pbucket(mp, mdid_type, dxl_bucket);
	//	buckets->Append(bucket);
	//}
	CDouble null_freq = pmdcolstats->GetNullFreq();
	CDouble distinct_remaining = pmdcolstats->GetDistinctRemain();
	CDouble freq_remaining = pmdcolstats->GetFreqRemain();
	CHistogram *histogram = GPOS_NEW(mp) CHistogram(mp, buckets, true, null_freq, distinct_remaining, freq_remaining, is_col_stats_missing);
	GPOS_ASSERT_IMP(fBoolType, 3 >= histogram->GetNumDistinct() - CStatistics::Epsilon);
	return histogram;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Pbucket
//
//	@doc:
//		Construct a typed bucket from a DXL bucket
//
//---------------------------------------------------------------------------
/*
CBucket* CMDAccessor::Pbucket(CMemoryPool *mp, IMDId *mdid_type,
					 const CDXLBucket *dxl_bucket)
{
	IDatum *pdatumLower =
		GetDatum(mp, mdid_type, dxl_bucket->GetDXLDatumLower());
	IDatum *pdatumUpper =
		GetDatum(mp, mdid_type, dxl_bucket->GetDXLDatumUpper());

	CPoint *bucket_lower_bound = GPOS_NEW(mp) CPoint(pdatumLower);
	CPoint *bucket_upper_bound = GPOS_NEW(mp) CPoint(pdatumUpper);

	return GPOS_NEW(mp)
		CBucket(bucket_lower_bound, bucket_upper_bound,
				dxl_bucket->IsLowerClosed(), dxl_bucket->IsUpperClosed(),
				dxl_bucket->GetFrequency(), dxl_bucket->GetNumDistinct());
}
*/
//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::GetDatum
//
//	@doc:
//		Construct a typed bucket from a DXL bucket
//
//---------------------------------------------------------------------------
/*
IDatum* CMDAccessor::GetDatum(CMemoryPool *mp, IMDId *mdid_type, const CDXLDatum *dxl_datum)
{
	const IMDType *pmdtype = RetrieveType(mdid_type);

	return pmdtype->GetDatumForDXLDatum(mp, dxl_datum);
}
*/

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::Serialize
//
//	@doc:
//		Serialize MD object into provided stream
//
//---------------------------------------------------------------------------
void
CMDAccessor::Serialize(COstream &oos)
{
	ULONG nentries = m_shtCacheAccessors.Size();
	IMDCacheObject **cacheEntries;
	CAutoRg<IMDCacheObject *> aCacheEntries;
	ULONG ul;

	// Iterate the hash table and insert all entries to the array.
	// The iterator holds a lock on the hash table, so we must not
	// do anything non-trivial that might e.g. allocate memory,
	// while iterating.
	cacheEntries = GPOS_NEW_ARRAY(m_mp, IMDCacheObject *, nentries);
	aCacheEntries = cacheEntries;
	{
		MDHTIter mdhtit(m_shtCacheAccessors);
		ul = 0;
		while (mdhtit.Advance())
		{
			MDHTIterAccessor mdhtitacc(mdhtit);
			SMDAccessorElem *pmdaccelem = mdhtitacc.Value();
			GPOS_ASSERT(NULL != pmdaccelem);
			cacheEntries[ul++] = pmdaccelem->GetImdObj();
		}
		GPOS_ASSERT(ul == nentries);
	}

	// Now that we're done iterating and no longer hold the lock,
	// serialize the entries.
	for (ul = 0; ul < nentries; ul++)
		oos << cacheEntries[ul]->GetStrRepr()->GetBuffer();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAccessor::SerializeSysid
//
//	@doc:
//		Serialize the system ids into provided stream
//
//---------------------------------------------------------------------------
void
CMDAccessor::SerializeSysid(COstream &oos)
{
	ULONG ul = 0;
	MDPHTIter mdhtit(m_shtProviders);

	while (mdhtit.Advance())
	{
		MDPHTIterAccessor mdhtitacc(mdhtit);
		SMDProviderElem *pmdpelem = mdhtitacc.Value();
		CSystemId sysid = pmdpelem->Sysid();


		WCHAR wszSysId[GPDXL_MDID_LENGTH];
		CWStringStatic str(wszSysId, GPOS_ARRAY_SIZE(wszSysId));

		if (0 < ul)
		{
			str.AppendFormat(GPOS_WSZ_LIT("%s"), ",");
		}

		str.AppendFormat(GPOS_WSZ_LIT("%d.%ls"), sysid.MdidType(),
						 sysid.GetBuffer());

		oos << str.GetBuffer();
		ul++;
	}
}