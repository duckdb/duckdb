//---------------------------------------------------------------------------
//	@filename:
//		CMDIdGPDB.cpp
//
//	@doc:
//		Implementation of metadata identifiers
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
using namespace gpos;
using namespace gpmd;

// initialize static members
// invalid key
CMDIdGPDB CMDIdGPDB::m_mdid_invalid_key(0, 0, 0);

// int2 mdid
CMDIdGPDB CMDIdGPDB::m_mdid_int2(GPDB_INT2);

// int4 mdid
CMDIdGPDB CMDIdGPDB::m_mdid_int4(GPDB_INT4);

// int8 mdid
CMDIdGPDB CMDIdGPDB::m_mdid_int8(GPDB_INT8);

// bool mdid
CMDIdGPDB CMDIdGPDB::m_mdid_bool(GPDB_BOOL);

// oid mdid
CMDIdGPDB CMDIdGPDB::m_mdid_oid(GPDB_OID);

// numeric mdid
CMDIdGPDB CMDIdGPDB::m_mdid_numeric(GPDB_NUMERIC);

// date mdid
CMDIdGPDB CMDIdGPDB::m_mdid_date(GPDB_DATE);

// time mdid
CMDIdGPDB CMDIdGPDB::m_mdid_time(GPDB_TIME);

// time with time zone mdid
CMDIdGPDB CMDIdGPDB::m_mdid_timeTz(GPDB_TIMETZ);

// timestamp mdid
CMDIdGPDB CMDIdGPDB::m_mdid_timestamp(GPDB_TIMESTAMP);

// timestamp with time zone mdid
CMDIdGPDB CMDIdGPDB::m_mdid_timestampTz(GPDB_TIMESTAMPTZ);

// absolute time mdid
CMDIdGPDB CMDIdGPDB::m_mdid_abs_time(GPDB_ABSTIME);

// relative time mdid
CMDIdGPDB CMDIdGPDB::m_mdid_relative_time(GPDB_RELTIME);

// interval mdid
CMDIdGPDB CMDIdGPDB::m_mdid_interval(GPDB_INTERVAL);

// time interval mdid
CMDIdGPDB CMDIdGPDB::m_mdid_time_interval(GPDB_TIMEINTERVAL);

// char mdid
CMDIdGPDB CMDIdGPDB::m_mdid_char(GPDB_SINGLE_CHAR);

// bpchar mdid
CMDIdGPDB CMDIdGPDB::m_mdid_bpchar(GPDB_CHAR);

// varchar mdid
CMDIdGPDB CMDIdGPDB::m_mdid_varchar(GPDB_VARCHAR);

// text mdid
CMDIdGPDB CMDIdGPDB::m_mdid_text(GPDB_TEXT);

// text mdid
CMDIdGPDB CMDIdGPDB::m_mdid_name(GPDB_NAME);

// float4 mdid
CMDIdGPDB CMDIdGPDB::m_mdid_float4(GPDB_FLOAT4);

// float8 mdid
CMDIdGPDB CMDIdGPDB::m_mdid_float8(GPDB_FLOAT8);

// cash mdid
CMDIdGPDB CMDIdGPDB::m_mdid_cash(GPDB_CASH);

// inet mdid
CMDIdGPDB CMDIdGPDB::m_mdid_inet(GPDB_INET);

// cidr mdid
CMDIdGPDB CMDIdGPDB::m_mdid_cidr(GPDB_CIDR);

// macaddr mdid
CMDIdGPDB CMDIdGPDB::m_mdid_macaddr(GPDB_MACADDR);

// count(*) mdid
CMDIdGPDB CMDIdGPDB::m_mdid_count_star(GPDB_COUNT_STAR);

// count(Any) mdid
CMDIdGPDB CMDIdGPDB::m_mdid_count_any(GPDB_COUNT_ANY);

// uuid mdid
CMDIdGPDB CMDIdGPDB::m_mdid_uuid(GPDB_UUID);

// unknown mdid
CMDIdGPDB CMDIdGPDB::m_mdid_unknown(GPDB_UNKNOWN);

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::CMDIdGPDB
//
//	@doc:
//		Constructs a metadata identifier with specified oid and default version
//		of 1.0
//
//---------------------------------------------------------------------------
CMDIdGPDB::CMDIdGPDB(CSystemId sysid, OID oid)
	: m_sysid(sysid),
	  m_oid(oid),
	  m_major_version(1),
	  m_minor_version(0),
	  m_str(m_mdid_array, GPOS_ARRAY_SIZE(m_mdid_array))
{
	if (CMDIdGPDB::m_mdid_invalid_key.Oid() == oid)
	{
		// construct an invalid mdid 0.0.0
		m_major_version = 0;
	}

	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::CMDIdGPDB
//
//	@doc:
//		Constructs a metadata identifier with specified oid and default version
//		of 1.0
//
//---------------------------------------------------------------------------
CMDIdGPDB::CMDIdGPDB(OID oid)
	: m_sysid(IMDId::EmdidGPDB, GPMD_GPDB_SYSID),
	  m_oid(oid),
	  m_major_version(1),
	  m_minor_version(0),
	  m_str(m_mdid_array, GPOS_ARRAY_SIZE(m_mdid_array))
{
	if (CMDIdGPDB::m_mdid_invalid_key.Oid() == oid)
	{
		// construct an invalid mdid 0.0.0
		m_major_version = 0;
	}

	// TODO:  - Jan 31, 2012; supply system id in constructor

	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::CMDIdGPDB
//
//	@doc:
//		Constructs a metadata identifier
//
//---------------------------------------------------------------------------
CMDIdGPDB::CMDIdGPDB(OID oid, ULONG version_major, ULONG version_minor)
	: m_sysid(IMDId::EmdidGPDB, GPMD_GPDB_SYSID),
	  m_oid(oid),
	  m_major_version(version_major),
	  m_minor_version(version_minor),
	  m_str(m_mdid_array, GPOS_ARRAY_SIZE(m_mdid_array))
{
	// TODO:  - Jan 31, 2012; supply system id in constructor
	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::CMDIdGPDB
//
//	@doc:
//		Copy constructor
//
//---------------------------------------------------------------------------
CMDIdGPDB::CMDIdGPDB(const CMDIdGPDB &mdid_source)
	: IMDId(),
	  m_sysid(mdid_source.Sysid()),
	  m_oid(mdid_source.Oid()),
	  m_major_version(mdid_source.VersionMajor()),
	  m_minor_version(mdid_source.VersionMinor()),
	  m_str(m_mdid_array, GPOS_ARRAY_SIZE(m_mdid_array))
{
	GPOS_ASSERT(mdid_source.IsValid());
	GPOS_ASSERT(IMDId::EmdidGPDB == mdid_source.MdidType());

	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void
CMDIdGPDB::Serialize()
{
	m_str.Reset();
	// serialize mdid as SystemType.Oid.Major.Minor
	m_str.AppendFormat(GPOS_WSZ_LIT("%d.%d.%d.%d"), MdidType(), m_oid,
					   m_major_version, m_minor_version);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::GetBuffer
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR *
CMDIdGPDB::GetBuffer() const
{
	return m_str.GetBuffer();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::Oid
//
//	@doc:
//		Returns the object id
//
//---------------------------------------------------------------------------
OID
CMDIdGPDB::Oid() const
{
	return m_oid;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::VersionMajor
//
//	@doc:
//		Returns the object's major version
//
//---------------------------------------------------------------------------
ULONG
CMDIdGPDB::VersionMajor() const
{
	return m_major_version;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::VersionMinor
//
//	@doc:
//		Returns the object's minor version
//
//---------------------------------------------------------------------------
ULONG
CMDIdGPDB::VersionMinor() const
{
	return m_minor_version;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::Equals
//
//	@doc:
//		Checks if the version of the current object is compatible with another version
//		of the same object
//
//---------------------------------------------------------------------------
BOOL
CMDIdGPDB::Equals(const IMDId *mdid) const
{
	if (NULL == mdid || EmdidGPDB != mdid->MdidType())
	{
		return false;
	}

	const CMDIdGPDB *mdidGPDB =
		static_cast<CMDIdGPDB *>(const_cast<IMDId *>(mdid));

	return (m_oid == mdidGPDB->Oid() &&
			m_major_version == mdidGPDB->VersionMajor() &&
			m_minor_version == mdidGPDB->VersionMinor());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::IsValid
//
//	@doc:
//		Is the mdid valid
//
//---------------------------------------------------------------------------
BOOL
CMDIdGPDB::IsValid() const
{
	return !Equals(&CMDIdGPDB::m_mdid_invalid_key);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream & CMDIdGPDB::OsPrint(IOstream &os) const
{
	os << "(" << Oid() << "," << VersionMajor() << "." << VersionMinor() << ")";
	return os;
}