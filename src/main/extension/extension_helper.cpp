#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"

#if defined(BUILD_ICU_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define ICU_STATICALLY_LOADED true
#include "icu-extension.hpp"
#else
#define ICU_STATICALLY_LOADED false
#endif

#if defined(BUILD_PARQUET_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define PARQUET_STATICALLY_LOADED true
#include "parquet-extension.hpp"
#else
#define PARQUET_STATICALLY_LOADED false
#endif

#if defined(BUILD_TPCH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define TPCH_STATICALLY_LOADED true
#include "tpch-extension.hpp"
#else
#define TPCH_STATICALLY_LOADED false
#endif

#if defined(BUILD_TPCDS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define TPCDS_STATICALLY_LOADED true
#include "tpcds-extension.hpp"
#else
#define TPCDS_STATICALLY_LOADED false
#endif

#if defined(BUILD_SUBSTRAIT_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define SUBSTRAIT_STATICALLY_LOADED true
#include "substrait-extension.hpp"
#else
#define SUBSTRAIT_STATICALLY_LOADED false
#endif

#if defined(BUILD_FTS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define FTS_STATICALLY_LOADED true
#include "fts-extension.hpp"
#else
#define FTS_STATICALLY_LOADED false
#endif

#if defined(BUILD_HTTPFS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define HTTPFS_STATICALLY_LOADED true
#include "httpfs-extension.hpp"
#else
#define HTTPFS_STATICALLY_LOADED false
#endif

#if defined(BUILD_VISUALIZER_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "visualizer-extension.hpp"
#endif

#if defined(BUILD_JSON_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define JSON_STATICALLY_LOADED true
#include "json-extension.hpp"
#else
#define JSON_STATICALLY_LOADED false
#endif

#if defined(BUILD_EXCEL_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "excel-extension.hpp"
#endif

#if defined(BUILD_SQLSMITH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "sqlsmith-extension.hpp"
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Default Extensions
//===--------------------------------------------------------------------===//
static DefaultExtension internal_extensions[] = {
    {"icu", "Adds support for time zones and collations using the ICU library", ICU_STATICALLY_LOADED},
    {"parquet", "Adds support for reading and writing parquet files", PARQUET_STATICALLY_LOADED},
    {"tpch", "Adds TPC-H data generation and query support", TPCH_STATICALLY_LOADED},
    {"tpcds", "Adds TPC-DS data generation and query support", TPCDS_STATICALLY_LOADED},
    {"substrait", "Adds support for the Substrait integration", SUBSTRAIT_STATICALLY_LOADED},
    {"fts", "Adds support for Full-Text Search Indexes", FTS_STATICALLY_LOADED},
    {"httpfs", "Adds support for reading and writing files over a HTTP(S) connection", HTTPFS_STATICALLY_LOADED},
    {"json", "Adds support for JSON operations", JSON_STATICALLY_LOADED},
    {"sqlite_scanner", "Adds support for reading SQLite database files", false},
    {"postgres_scanner", "Adds support for reading from a Postgres database", false},
    {nullptr, nullptr, false}};

idx_t ExtensionHelper::DefaultExtensionCount() {
	idx_t index;
	for (index = 0; internal_extensions[index].name != nullptr; index++) {
	}
	return index;
}

DefaultExtension ExtensionHelper::GetDefaultExtension(idx_t index) {
	D_ASSERT(index < DefaultExtensionCount());
	return internal_extensions[index];
}

//===--------------------------------------------------------------------===//
// Load Statically Compiled Extension
//===--------------------------------------------------------------------===//
void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	unordered_set<string> extensions {"parquet",   "icu",        "tpch", "tpcds", "fts",     "httpfs",
	                                  "substrait", "visualizer", "json", "excel", "sqlsmith"};
	for (auto &ext : extensions) {
		LoadExtensionInternal(db, ext, true);
	}
}

ExtensionLoadResult ExtensionHelper::LoadExtension(DuckDB &db, const std::string &extension) {
	return LoadExtensionInternal(db, extension, false);
}

ExtensionLoadResult ExtensionHelper::LoadExtensionInternal(DuckDB &db, const std::string &extension,
                                                           bool initial_load) {
#ifdef DUCKDB_TEST_REMOTE_INSTALL
	if (!initial_load && StringUtil::Contains(DUCKDB_TEST_REMOTE_INSTALL, extension)) {
		Connection con(db);
		auto result = con.Query("INSTALL " + extension);
		if (!result->success) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		result = con.Query("LOAD " + extension);
		if (!result->success) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		return ExtensionLoadResult::LOADED_EXTENSION;
	}
#endif
	if (extension == "parquet") {
#if PARQUET_STATICALLY_LOADED
		db.LoadExtension<ParquetExtension>();
#else
		// parquet extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "icu") {
#if ICU_STATICALLY_LOADED
		db.LoadExtension<ICUExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpch") {
#if TPCH_STATICALLY_LOADED
		db.LoadExtension<TPCHExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "substrait") {
#if SUBSTRAIT_STATICALLY_LOADED

		db.LoadExtension<SubstraitExtension>();
#else
		// substrait extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpcds") {
#if TPCDS_STATICALLY_LOADED
		db.LoadExtension<TPCDSExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "fts") {
#if FTS_STATICALLY_LOADED
		db.LoadExtension<FTSExtension>();
#else
		// fts extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "httpfs") {
#if HTTPFS_STATICALLY_LOADED
		db.LoadExtension<HTTPFsExtension>();
#else
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "visualizer") {
#if defined(BUILD_VISUALIZER_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<VisualizerExtension>();
#else
		// visualizer extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "json") {
#if JSON_STATICALLY_LOADED
		db.LoadExtension<JSONExtension>();
#else
		// json extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "excel") {
#if defined(BUILD_EXCEL_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<EXCELExtension>();
#else
		// excel extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "sqlsmith") {
#if defined(BUILD_SQLSMITH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<SQLSmithExtension>();
#else
		// excel extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else {
		// unknown extension
		return ExtensionLoadResult::EXTENSION_UNKNOWN;
	}
	return ExtensionLoadResult::LOADED_EXTENSION;
}

static std::vector<std::string> public_keys = {
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6aZuHUa1cLR9YDDYaEfi
UDbWY8m2t7b71S+k1ZkXfHqu+5drAxm+dIDzdOHOKZSIdwnJbT3sSqwFoG6PlXF3
g3dsJjax5qESIhbVvf98nyipwNINxoyHCkcCIPkX17QP2xpnT7V59+CqcfDJXLqB
ymjqoFSlaH8dUCHybM4OXlWnAtVHW/nmw0khF8CetcWn4LxaTUHptByaBz8CasSs
gWpXgSfaHc3R9eArsYhtsVFGyL/DEWgkEHWolxY3Llenhgm/zOf3s7PsAMe7EJX4
qlSgiXE6OVBXnqd85z4k20lCw/LAOe5hoTMmRWXIj74MudWe2U91J6GrrGEZa7zT
7QIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAq8Gg1S/LI6ApMAYsFc9m
PrkFIY+nc0LXSpxm77twU8D5M0Xkz/Av4f88DQmj1OE3164bEtR7sl7xDPZojFHj
YYyucJxEI97l5OU1d3Pc1BdKXL4+mnW5FlUGj218u8qD+G1hrkySXQkrUzIjPPNw
o6knF3G/xqQF+KI+tc7ajnTni8CAlnUSxfnstycqbVS86m238PLASVPK9/SmIRgO
XCEV+ZNMlerq8EwsW4cJPHH0oNVMcaG+QT4z79roW1rbJghn9ubAVdQU6VLUAikI
b8keUyY+D0XdY9DpDBeiorb1qPYt8BPLOAQrIUAw1CgpMM9KFp9TNvW47KcG4bcB
dQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyYATA9KOQ0Azf97QAPfY
Jc/WeZyE4E1qlRgKWKqNtYSXZqk5At0V7w2ntAWtYSpczFrVepCJ0oPMDpZTigEr
NgOgfo5LEhPx5XmtCf62xY/xL3kgtfz9Mm5TBkuQy4KwY4z1npGr4NYYDXtF7kkf
LQE+FnD8Yr4E0wHBib7ey7aeeKWmwqvUjzDqG+TzaqwzO/RCUsSctqSS0t1oo2hv
4q1ofanUXsV8MXk/ujtgxu7WkVvfiSpK1zRazgeZjcrQFO9qL/pla0vBUxa1U8He
GMLnL0oRfcMg7yKrbIMrvlEl2ZmiR9im44dXJWfY42quObwr1PuEkEoCMcMisSWl
jwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4RvbWx3zLblDHH/lGUF5
Q512MT+v3YPriuibROMllv8WiCLAMeJ0QXbVaIzBOeHDeLx8yvoZZN+TENKxtT6u
IfMMneUzxHBqy0AQNfIsSsOnG5nqoeE/AwbS6VqCdH1aLfoCoPffacHYa0XvTcsi
aVlZfr+UzJS+ty8pRmFVi1UKSOADDdK8XfIovJl/zMP2TxYX2Y3fnjeLtl8Sqs2e
P+eHDoy7Wi4EPTyY7tNTCfxwKNHn1HQ5yrv5dgvMxFWIWXGz24yikFvtwLGHe8uJ
Wi+fBX+0PF0diZ6pIthZ149VU8qCqYAXjgpxZ0EZdrsiF6Ewz0cfg20SYApFcmW4
pwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyhd5AfwrUohG3O4DE0K9
O3FmgB7zE4aDnkL8UUfGCh5kdP8q7ewMjekY+c6LwWOmpdJpSwqhfV1q5ZU1l6rk
3hlt03LO3sgs28kcfOVH15hqfxts6Sg5KcRjxStE50ORmXGwXDcS9vqkJ60J1EHA
lcZqbCRSO73ZPLhdepfd0/C6tM0L7Ge6cAE62/MTmYNGv8fDzwQr/kYIJMdoS8Zp
thRpctFZJtPs3b0fffZA/TCLVKMvEVgTWs48751qKid7N/Lm/iEGx/tOf4o23Nec
Pz1IQaGLP+UOLVQbqQBHJWNOqigm7kWhDgs3N4YagWgxPEQ0WVLtFji/ZjlKZc7h
dwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnFDg3LhyV6BVE2Z3zQvN
6urrKvPhygTa5+wIPGwYTzJ8DfGALqlsX3VOXMvcJTca6SbuwwkoXHuSU5wQxfcs
bt4jTXD3NIoRwQPl+D9IbgIMuX0ACl27rJmr/f9zkY7qui4k1X82pQkxBe+/qJ4r
TBwVNONVx1fekTMnSCEhwg5yU3TNbkObu0qlQeJfuMWLDQbW/8v/qfr/Nz0JqHDN
yYKfKvFMlORxyJYiOyeOsbzNGEhkGQGOmKhRUhS35kD+oA0jqwPwMCM9O4kFg/L8
iZbpBBX2By1K3msejWMRAewTOyPas6YMQOYq9BMmWQqzVtG5xcaSJwN/YnMpJyqb
sQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1z0RU8vGrfEkrscEoZKA
GiOcGh2EMcKwjQpl4nKuR9H4o/dg+CZregVSHg7MP2f8mhLZZyoFev49oWOV4Rmi
qs99UNxm7DyKW1fF1ovowsUW5lsDoKYLvpuzHo0s4laiV4AnIYP7tHGLdzsnK2Os
Cp5dSuMwKHPZ9N25hXxFB/dRrAdIiXHvbSqr4N29XzfQloQpL3bGHLKY6guFHluH
X5dJ9eirVakWWou7BR2rnD0k9vER6oRdVnJ6YKb5uhWEOQ3NmV961oyr+uiDTcep
qqtGHWuFhENixtiWGjFJJcACwqxEAW3bz9lyrfnPDsHSW/rlQVDIAkik+fOp+R7L
kQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxwO27e1vnbNcpiDg7Wwx
K/w5aEGukXotu3529ieq+O39H0+Bak4vIbzGhDUh3/ElmxaFMAs4PYrWe/hc2WFD
H4JCOoFIn4y9gQeE855DGGFgeIVd1BnSs5S+5wUEMxLNyHdHSmINN6FsoZ535iUg
KdYjRh1iZevezg7ln8o/O36uthu925ehFBXSy6jLJgQlwmq0KxZJE0OAZhuDBM60
MtIunNa/e5y+Gw3GknFwtRLmn/nEckZx1nEtepYvvUa7UGy+8KuGuhOerCZTutbG
k8liCVgGenRve8unA2LrBbpL+AUf3CrZU/uAxxTqWmw6Z/S6TeW5ozeeyOCh8ii6
TwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsGIFOfIQ4RI5qu4klOxf
ge6eXwBMAkuTXyhyIIJDtE8CurnwQvUXVlt+Kf0SfuIFW6MY5ErcWE/vMFbc81IR
9wByOAAV2CTyiLGZT63uE8pN6FSHd6yGYCLjXd3P3cnP3Qj5pBncpLuAUDfHG4wP
bs9jIADw3HysD+eCNja8p7ZC7CzWxTcO7HsEu9deAAU19YywdpagXvQ0pJ9zV5qU
jrHxBygl31t6TmmX+3d+azjGu9Hu36E+5wcSOOhuwAFXDejb40Ixv53ItJ3fZzzH
PF2nj9sQvQ8c5ptjyOvQCBRdqkEWXIVHClxqWb+o59pDIh1G0UGcmiDN7K9Gz5HA
ZQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt9uUnlW/CoUXT68yaZh9
SeXHzGRCPNEI98Tara+dgYxDX1z7nfOh8o15liT0QsAzx34EewZOxcKCNiV/dZX5
z4clCkD8uUbZut6IVx8Eu+7Qcd5jZthRc6hQrN9Ltv7ZQEh7KGXOHa53kT2K01ws
4jbVmd/7Nx7y0Yyqhja01pIu/CUaTkODfQxBXwriLdIzp7y/iJeF/TLqCwZWHKQx
QOZnsPEveB1F00Va9MeAtTlXFUJ/TQXquqTjeLj4HuIRtbyuNgWoc0JyF+mcafAl
bnrNEBIfxZhAT81aUCIAzRJp6AqfdeZxnZ/WwohtZQZLXAxFQPTWCcP+Z9M7OIQL
WwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA56NhfACkeCyZM07l2wmd
iTp24E2tLLKU3iByKlIRWRAvXsOejRMJTHTNHWa3cQ7uLP++Tf2St7ksNsyPMNZy
9QRTLNCYr9rN9loLwdb2sMWxFBwwzCaAOTahGI7GJQy30UB7FEND0X/5U2rZvQij
Q6K+O4aa+K9M5qyOHNMmXywmTnAgWKNaNxQHPRtD2+dSj60T6zXdtIuCrPfcNGg5
gj07qWGEXX83V/L7nSqCiIVYg/wqds1x52Yjk1nhXYNBTqlnhmOd8LynGxz/sXC7
h2Q9XsHjXIChW4FHyLIOl6b4zPMBSxzCigYm3QZJWfAkZv5PBRtnq7vhYOLHzLQj
CwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmfPLe0IWGYC0MZC6YiM3
QGfhT6zSKB0I2DW44nlBlWUcF+32jW2bFJtgE76qGGKFeU4kJBWYr99ufHoAodNg
M1Ehl/JfQ5KmbC1WIqnFTrgbmqJde79jeCvCpbFLuqnzidwO1PbXDbfRFQcgWaXT
mDVLNNVmLxA0GkCv+kydE2gtcOD9BDceg7F/56TDvclyI5QqAnjE2XIRMPZlXQP4
oF2kgz4Cn7LxLHYmkU2sS9NYLzHoyUqFplWlxkQjA4eQ0neutV1Ydmc1IX8W7R38
A7nFtaT8iI8w6Vkv7ijYN6xf5cVBPKZ3Dv7AdwPet86JD5mf5v+r7iwg5xl3r77Z
iwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoB1kWsX8YmCcFOD9ilBY
xK076HmUAN026uJ8JpmU9Hz+QT1FNXOsnj1h2G6U6btYVIdHUTHy/BvAumrDKqRz
qcEAzCuhxUjPjss54a/Zqu6nQcoIPHuG/Er39oZHIVkPR1WCvWj8wmyYv6T//dPH
unO6tW29sXXxS+J1Gah6vpbtJw1pI/liah1DZzb13KWPDI6ZzviTNnW4S05r6js/
30He+Yud6aywrdaP/7G90qcrteEFcjFy4Xf+5vG960oKoGoDplwX5poay1oCP9tb
g8AC8VSRAGi3oviTeSWZcrLXS8AtJhGvF48cXQj2q+8YeVKVDpH6fPQxJ9Sh9aeU
awIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4NTMAIYIlCMID00ufy/I
AZXc8pocDx9N1Q5x5/cL3aIpLmx02AKo9BvTJaJuHiTjlwYhPtlhIrHV4HUVTkOX
sISp8B8v9i2I1RIvCTAcvy3gcH6rdRWZ0cdTUiMEqnnxBX9zdzl8oMzZcyauv19D
BeqJvzflIT96b8g8K3mvgJHs9a1j9f0gN8FuTA0c52DouKnrh8UwH7mlrumYerJw
6goJGQuK1HEOt6bcQuvogkbgJWOoEYwjNrPwQvIcP4wyrgSnOHg1yXOFE84oVynJ
czQEOz9ke42I3h8wrnQxilEYBVo2uX8MenqTyfGnE32lPRt3Wv1iEVQls8Cxiuy2
CQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3bUtfp66OtRyvIF/oucn
id8mo7gvbNEH04QMLO3Ok43dlWgWI3hekJAqOYc0mvoI5anqr98h8FI7aCYZm/bY
vpz0I1aXBaEPh3aWh8f/w9HME7ykBvmhMe3J+VFGWWL4eswfRl//GCtnSMBzDFhM
SaQOTvADWHkC0njeI5yXjf/lNm6fMACP1cnhuvCtnx7VP/DAtvUk9usDKG56MJnZ
UoVM3HHjbJeRwxCdlSWe12ilCdwMRKSDY92Hk38/zBLenH04C3HRQLjBGewACUmx
uvNInehZ4kSYFGa+7UxBxFtzJhlKzGR73qUjpWzZivCe1K0WfRVP5IWsKNCCESJ/
nQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyV2dE/CRUAUE8ybq/DoS
Lc7QlYXh04K+McbhN724TbHahLTuDk5mR5TAunA8Nea4euRzknKdMFAz1eh9gyy3
5x4UfXQW1fIZqNo6WNrGxYJgWAXU+pov+OvxsMQWzqS4jrTHDHbblCCLKp1akwJk
aFNyqgjAL373PcqXC+XAn8vHx4xHFoFP5lq4lLcJCOW5ee9v9El3w0USLwS+t1cF
RY3kuV6Njlr4zsRH9iM6/zaSuCALYWJ/JrPEurSJXzFZnWsvn6aQdeNeAn08+z0F
k2NwaauEo0xmLqzqTRGzjHqKKmeefN3/+M/FN2FrApDlxWQfhD2Y3USdAiN547Nj
1wIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvm2+kTrEQWZXuxhWzBdl
PCbQGqbrukbeS6JKSlQLJDC8ayZIxFxatqg1Q8UPyv89MVRsHOGlG1OqFaOEtPjQ
Oo6j/moFwB4GPyJhJHOGpCKa4CLB5clhfDCLJw6ty7PcDU3T6yW4X4Qc5k4LRRWy
yzC8lVHfBdarN+1iEe0ALMOGoeiJjVn6i/AFxktRwgd8njqv/oWQyfjJZXkNMsb6
7ZDxNVAUrp/WXpE4Kq694bB9xa/pWsqv7FjQJUgTnEzvbN+qXnVPtA7dHcOYYJ8Z
SbrJUfHrf8TS5B54AiopFpWG+hIbjqqdigqabBqFpmjiRDZgDy4zJJj52xJZMnrp
rwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwEAcVmY3589O02pLA22f
MlarLyJUgy0BeJDG5AUsi17ct8sHZzRiv9zKQVCBk1CtZY//jyqnrM7iCBLWsyby
TiTOtGYHHApaLnNjjtaHdQ6zplhbc3g2XLy+4ab8GNKG3zc8iXpsQM6r+JO5n9pm
V9vollz9dkFxS9l+1P17lZdIgCh9O3EIFJv5QCd5c9l2ezHAan2OhkWhiDtldnH/
MfRXbz7X5sqlwWLa/jhPtvY45x7dZaCHGqNzbupQZs0vHnAVdDu3vAWDmT/3sXHG
vmGxswKA9tPU0prSvQWLz4LUCnGi/cC5R+fiu+fovFM/BwvaGtqBFIF/1oWVq7bZ
4wIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA25qGwNO1+qHygC8mjm8L
3I66mV/IzslgBDHC91mE8YcI5Fq0sdrtsbUhK3z89wIN/zOhbHX0NEiXm2GxUnsI
vb5tDZXAh7AbTnXTMVbxO/e/8sPLUiObGjDvjVzyzrxOeG87yK/oIiilwk9wTsIb
wMn2Grj4ht9gVKx3oGHYV7STNdWBlzSaJj4Ou7+5M1InjPDRFZG1K31D2d3IHByX
lmcRPZtPFTa5C1uVJw00fI4F4uEFlPclZQlR5yA0G9v+0uDgLcjIUB4eqwMthUWc
dHhlmrPp04LI19eksWHCtG30RzmUaxDiIC7J2Ut0zHDqUe7aXn8tOVI7dE9tTKQD
KQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA7EC2bx7aRnf3TcRg5gmw
QOKNCUheCelK8hoXLMsKSJqmufyJ+IHUejpXGOpvyYRbACiJ5GiNcww20MVpTBU7
YESWB2QSU2eEJJXMq84qsZSO8WGmAuKpUckI+hNHKQYJBEDOougV6/vVVEm5c5bc
SLWQo0+/ciQ21Zwz5SwimX8ep1YpqYirO04gcyGZzAfGboXRvdUwA+1bZvuUXdKC
4zsCw2QALlcVpzPwjB5mqA/3a+SPgdLAiLOwWXFDRMnQw44UjsnPJFoXgEZiUpZm
EMS5gLv50CzQqJXK9mNzPuYXNUIc4Pw4ssVWe0OfN3Od90gl5uFUwk/G9lWSYnBN
3wIDAQAB
-----END PUBLIC KEY-----
)"};

const vector<string> ExtensionHelper::GetPublicKeys() {
	return public_keys;
}

} // namespace duckdb
