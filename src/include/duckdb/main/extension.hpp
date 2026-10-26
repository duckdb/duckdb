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
class ExtensionLoader;

//! The Extension class is the base class used to define extensions
class Extension {
public:
	DUCKDB_API virtual ~Extension();

	DUCKDB_API virtual void Load(ExtensionLoader &db) = 0;
	DUCKDB_API virtual std::string Name() = 0;
	DUCKDB_API virtual std::string Version() const {
		return "";
	}
	DUCKDB_API static const char *DefaultVersion();
};

enum class ExtensionABIType : uint8_t {
	UNKNOWN = 0,
	//! Uses C++ ABI, version needs to match precisely
	CPP = 1,
	//! Uses C ABI, version needs to be equal or higher. The CAPI version major selects the API family: 1 uses the
	//! duckdb_ext_api_v1 struct and the <name>_init_c_api entrypoint, 2 the duckdb_ext_api_v2 struct and
	//! <name>_init_c_api_v2
	C_STRUCT = 2,
	//! Uses C ABI using the duckdb_ext_api_v2 struct including "unstable" functions, version needs to match precisely.
	//! Everything that was unstable in the V1 API was stabilized into v1.5.6, so "unstable" now always means V2
	C_STRUCT_UNSTABLE = 3
};

//! The parsed extension metadata footer
struct ParsedExtensionMetaData {
	// The footer trails the extension file as [METADATA_SIZE bytes of metadata] + [signature_size bytes of signature].
	// The metadata block is always METADATA_SIZE bytes and directly precedes the trailing signature; only the trailing
	// signature length varies with the RSA key size (2048 -> 256, 3072 -> 384). FOOTER_SIZE is the legacy total for a
	// 256-byte signature (256 metadata + 256 signature) and stays the probe window for the legacy-first detection.
	static constexpr const idx_t FOOTER_SIZE = 512;
	static constexpr const idx_t METADATA_SIZE = 256;
	//! Legacy/default signature length (RSA-2048). Kept as the default so unmarked legacy footers parse unchanged.
	static constexpr const idx_t SIGNATURE_SIZE = 256;
	//! Signature length for an RSA-3072 key.
	static constexpr const idx_t SIGNATURE_SIZE_RSA3072 = 384;
	static constexpr const char *EXPECTED_MAGIC_VALUE = {
	    "4\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"};

	string magic_value;

	ExtensionABIType abi_type;

	string platform;
	// (For ExtensionABIType::CPP or ExtensionABIType::C_STRUCT_UNSTABLE) the DuckDB version this extension is compiled
	// for
	string duckdb_version;
	// (only for ExtensionABIType::C_STRUCT) the CAPI version of the C_STRUCT (Currently interpreted as the minimum
	// DuckDB version)
	string duckdb_capi_version;
	string extension_version;
	string signature;
	string extension_abi_metadata;

	//! The resolved trailing-signature length in bytes (256 for legacy RSA-2048, 384 for RSA-3072). Derived from the
	//! signed size-marker in reserved metadata field index 6, defaulting to the legacy length.
	idx_t signature_size = SIGNATURE_SIZE;
	//! Non-empty when the footer carried an unrecognized/invalid signature-size marker. Surfaced by
	//! GetInvalidMetadataError() so the load is rejected, since the marker lives in the signed region.
	string metadata_error;

	bool AppearsValid() {
		return magic_value == EXPECTED_MAGIC_VALUE;
	}

	// Returns an error string describing which parts of the metadata are mismatcheds
	string GetInvalidMetadataError();
};

struct VersioningUtils {
	//! Note: only supports format v{major}.{minor}.{patch}
	static bool ParseSemver(string &semver, idx_t &major_out, idx_t &minor_out, idx_t &patch_out);

	static bool IsReleaseVersion(const string &version_tag);

	//! Note: only supports format v{major}.{minor}.{patch}
	//! The major selects which C API family the version is checked against: 1 against duckdb_ext_api_v1, 2 against
	//! duckdb_ext_api_v2. Any other major is unsupported.
	static bool IsSupportedCAPIVersion(string &capi_version_string);
	static bool IsSupportedCAPIVersion(idx_t major, idx_t minor, idx_t patch);

	//! Whether a C_STRUCT extension built against this CAPI version uses the V2 API
	static bool IsCAPIV2Version(const string &capi_version_string);
};

} // namespace duckdb
