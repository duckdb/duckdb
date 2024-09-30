//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class DuckDB;

//! The Extension class is the base class used to define extensions
class Extension {
public:
	DUCKDB_API virtual ~Extension();

	DUCKDB_API virtual void Load(DuckDB &db) = 0;
	DUCKDB_API virtual std::string Name() = 0;
	DUCKDB_API virtual std::string Version() const {
		return "";
	}
};

enum class ExtensionABIType : uint8_t {
	UNKNOWN = 0,
	CPP = 1,
	C_STRUCT = 2,
};

//! The parsed extension metadata footer
struct ParsedExtensionMetaData {
	static constexpr const idx_t FOOTER_SIZE = 512;
	static constexpr const idx_t SIGNATURE_SIZE = 256;
	static constexpr const char *EXPECTED_MAGIC_VALUE = {
	    "4\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"};

	string magic_value;

	ExtensionABIType abi_type;

	string platform;
	// (only for ExtensionABIType::CPP) the DuckDB version this extension is compiled for
	string duckdb_version;
	// (only for ExtensionABIType::C_STRUCT) the CAPI version of the C_STRUCT
	string duckdb_capi_version;
	string extension_version;
	string signature;
	string extension_abi_metadata;

	bool AppearsValid() {
		return magic_value == EXPECTED_MAGIC_VALUE;
	}

	// Returns an error string describing which parts of the metadata are mismatcheds
	string GetInvalidMetadataError();
};

struct VersioningUtils {
	//! Note: only supports format v{major}.{minor}.{patch}
	static bool ParseSemver(string &semver, idx_t &major_out, idx_t &minor_out, idx_t &patch_out);

	//! Note: only supports format v{major}.{minor}.{patch}
	static bool IsSupportedCAPIVersion(string &capi_version_string);
	static bool IsSupportedCAPIVersion(idx_t major, idx_t minor, idx_t patch);
};

} // namespace duckdb
