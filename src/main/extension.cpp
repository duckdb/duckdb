#include "duckdb/main/extension.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/capi/extension_api.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

Extension::~Extension() {
}

static string PrettyPrintString(const string &s) {
	string res = "";
	for (auto c : s) {
		if (StringUtil::CharacterIsAlpha(c) || StringUtil::CharacterIsDigit(c) || c == '_' || c == '-' || c == ' ' ||
		    c == '.') {
			res += c;
		} else {
			auto value = UnsafeNumericCast<uint8_t>(c);
			res += "\\x";
			uint8_t first = value / 16;
			if (first < 10) {
				res.push_back((char)('0' + first));
			} else {
				res.push_back((char)('a' + first - 10));
			}
			uint8_t second = value % 16;
			if (second < 10) {
				res.push_back((char)('0' + second));
			} else {
				res.push_back((char)('a' + second - 10));
			}
		}
	}
	return res;
}

string ParsedExtensionMetaData::GetInvalidMetadataError() {
	const string engine_platform = string(DuckDB::Platform());

	if (!AppearsValid()) {
		return "The file is not a DuckDB extension. The metadata at the end of the file is invalid";
	}

	string result;

	if (abi_type == ExtensionABIType::CPP) {
		const string engine_version = string(ExtensionHelper::GetVersionDirectoryName());

		if (engine_version != duckdb_version) {
			result += StringUtil::Format("The file was built for DuckDB version '%s', but we can only load extensions "
			                             "built for DuckDB version '%s'.",
			                             PrettyPrintString(duckdb_version), engine_version);
		}
	} else if (abi_type == ExtensionABIType::C_STRUCT) {

		if (!VersioningUtils::IsSupportedCAPIVersion(duckdb_capi_version)) {
			result +=
			    StringUtil::Format("The file was built for DuckDB C API version '%s', but we can only load extensions "
			                       "built for DuckDB C API 'v%lld.%lld.%lld' and lower.",
			                       duckdb_capi_version, DUCKDB_EXTENSION_API_VERSION_MAJOR,
			                       DUCKDB_EXTENSION_API_VERSION_MINOR, DUCKDB_EXTENSION_API_VERSION_PATCH);
		}
	} else {
		throw InternalException("Unknown ABI type for extension: " + extension_abi_metadata);
	}

	if (engine_platform != platform) {
		if (!result.empty()) {
			result += " Also, t";
		} else {
			result += "T";
		}
		result += StringUtil::Format(
		    "he file was built for the platform '%s', but we can only load extensions built for platform '%s'.",
		    PrettyPrintString(platform), engine_platform);
	}

	return result;
}

bool VersioningUtils::IsSupportedCAPIVersion(string &capi_version_string) {
	idx_t major, minor, patch;
	if (!ParseSemver(capi_version_string, major, minor, patch)) {
		return false;
	}

	return IsSupportedCAPIVersion(major, minor, patch);
}

bool VersioningUtils::IsSupportedCAPIVersion(idx_t major, idx_t minor, idx_t patch) {
	if (major > DUCKDB_EXTENSION_API_VERSION_MAJOR || minor > DUCKDB_EXTENSION_API_VERSION_MINOR ||
	    patch > DUCKDB_EXTENSION_API_VERSION_PATCH) {
		return false;
	}

	return true;
}

bool VersioningUtils::ParseSemver(string &semver, idx_t &major_out, idx_t &minor_out, idx_t &patch_out) {
	if (!StringUtil::StartsWith(semver, "v")) {
		return false;
	}

	auto without_v = semver.substr(1);

	auto split = StringUtil::Split(without_v, '.');

	if (split.size() != 3) {
		return false;
	}

	idx_t major, minor, patch;
	bool succeeded = true;

	succeeded &= TryCast::Operation<string_t, idx_t>(split[0], major);
	succeeded &= TryCast::Operation<string_t, idx_t>(split[1], minor);
	succeeded &= TryCast::Operation<string_t, idx_t>(split[2], patch);

	if (!succeeded) {
		return false;
	}

	major_out = major;
	minor_out = minor;
	patch_out = patch;

	return true;
}

} // namespace duckdb
