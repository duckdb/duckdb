#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/database.hpp"

#include <cstdint>

namespace duckdb {

struct PragmaVersionData : public GlobalTableFunctionState {
	PragmaVersionData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaVersionBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("library_version");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_id");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> PragmaVersionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaVersionData>();
}

static void PragmaVersionFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaVersionData>();
	if (data.finished) {
		// finished returning values
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, DuckDB::LibraryVersion());
	output.SetValue(1, 0, DuckDB::SourceID());
	data.finished = true;
}

void PragmaVersion::RegisterFunction(BuiltinFunctions &set) {
	TableFunction pragma_version("pragma_version", {}, PragmaVersionFunction);
	pragma_version.bind = PragmaVersionBind;
	pragma_version.init_global = PragmaVersionInit;
	set.AddFunction(pragma_version);
}

struct PragmaExtensionVersionData : public GlobalTableFunctionState {
	PragmaExtensionVersionData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaExtensionVersionBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("extension_folder");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("platform");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> PragmaExtensionVersionInit(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	return make_uniq<PragmaExtensionVersionData>();
}

static void PragmaExtensionVersionFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaExtensionVersionData>();
	if (data.finished) {
		// finished returning values
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, DuckDB::ExtensionFolder());
	output.SetValue(1, 0, DuckDB::Platform());
	data.finished = true;
}

void PragmaExtensionVersion::RegisterFunction(BuiltinFunctions &set) {
	TableFunction pragma_extension_version("pragma_extension_version", {}, PragmaExtensionVersionFunction);
	pragma_extension_version.bind = PragmaExtensionVersionBind;
	pragma_extension_version.init_global = PragmaExtensionVersionInit;
	set.AddFunction(pragma_extension_version);
}

idx_t DuckDB::StandardVectorSize() {
	return STANDARD_VECTOR_SIZE;
}

const char *DuckDB::SourceID() {
	return DUCKDB_SOURCE_ID;
}

const char *DuckDB::LibraryVersion() {
	return DUCKDB_VERSION;
}

static const string NormalizeVersionTag(const string &version_tag) {
	if (version_tag.length() > 0 && version_tag[0] != 'v') {
		return "v" + version_tag;
	}
	return version_tag;
}

static bool IsRelease(const string &version_tag) {
	return !StringUtil::Contains(version_tag, "-dev");
}

#define QUOTE_IMPL(x) #x
#define QUOTE_ME(x)   QUOTE_IMPL(x)

string DuckDB::ExtensionFolder() {
	string duckdb_version;
	if (IsRelease(DuckDB::LibraryVersion())) {
		duckdb_version = NormalizeVersionTag(DuckDB::LibraryVersion());
	} else {
		duckdb_version = DuckDB::SourceID();
	}
#if defined(DUCKDB_WASM_HASH)
	duckdb_version += "-wasm" + QUOTE_ME(DUCKDB_WASM_HASH);
#endif
	return duckdb_version;
}

string DuckDB::Platform() {
	string os = "linux";

#if defined(CUSTOM_PLATFORM)
	//	Iff CUSTOM_PLATFORM is defined, use that (quoted) as platform
	return QUOTE(CUSTOM_PLATFORM);
#endif
#if defined(DUCKDB_WASM_HASH)
	// DUCKDB_WASM requires CUSTOM_PLATFORM to be defined
	static_assert(0, "DUCKDB_WASM_HASH should rely on CUSTOM_PLATFORM being provided");
#endif

#if INTPTR_MAX == INT64_MAX
	string arch = "amd64";
#elif INTPTR_MAX == INT32_MAX
	string arch = "i686";
#else
#error Unknown pointer size or missing size macros!
#endif
	string postfix = "";

#ifdef _WIN32
	os = "windows";
#elif defined(__APPLE__)
	os = "osx";
#endif
#if defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
	arch = "arm64";
#endif

#if !defined(_GLIBCXX_USE_CXX11_ABI) || _GLIBCXX_USE_CXX11_ABI == 0
	if (os == "linux") {
		postfix = "_gcc4";
	}
#endif
#ifdef __MINGW32__
	postfix = "_mingw";
#endif
// this is used for the windows R builds which use a separate build environment
#ifdef DUCKDB_PLATFORM_RTOOLS
	postfix = "_rtools";
#endif
	return os + "_" + arch + postfix;
}

} // namespace duckdb
