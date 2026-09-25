#include "duckdb/common/dl.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_helper.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS
#include <emscripten.h>
#endif

#if defined(DUCKDB_DISABLE_EXTENSION_LOAD)
#define DUCKDB_EXTENSION_LOAD_DEFINE_WARNING                                                                           \
	"DUCKDB_DISABLE_EXTENSION_LOAD no longer disables loading and installing extensions: build with "                  \
	"ENABLE_EXTENSION_LOAD=OFF, or package_build.py with extension_load=False, to compile extension_load_none.cpp "    \
	"instead"
#if defined(_MSC_VER)
#pragma message("warning: " DUCKDB_EXTENSION_LOAD_DEFINE_WARNING)
#else
#pragma GCC warning DUCKDB_EXTENSION_LOAD_DEFINE_WARNING
#endif
#endif

namespace duckdb {

bool ExtensionHelper::SupportsExternalExtensions() {
	return true;
}

void *ExtensionHelper::OpenExtensionLibrary(const string &filename, const string &filebase) {
#ifdef WASM_LOADABLE_EXTENSIONS
	EM_ASM(
	    {
		    // Next few lines should arguably in separate JavaScript-land function call
		    // TODO: move them out / have them configurable
		    const xhr = new XMLHttpRequest();
		    xhr.open("GET", UTF8ToString($0), false);
		    xhr.responseType = "arraybuffer";
		    xhr.send(null);
		    var uInt8Array = xhr.response;
		    WebAssembly.validate(uInt8Array);
		    console.log('Loading extension ', UTF8ToString($1));

		    // Here we add the uInt8Array to Emscripten's filesystem, for it to be found by dlopen
		    FS.writeFile(UTF8ToString($1), new Uint8Array(uInt8Array));
	    },
	    filename.c_str(), filebase.c_str());
	auto dopen_from = filebase;
#else
	auto dopen_from = filename;
#endif

	auto lib_hdl = dlopen(dopen_from.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (!lib_hdl) {
		throw IOException("Extension \"%s\" could not be loaded: %s", filename, GetDLError());
	}
	return lib_hdl;
}

void *ExtensionHelper::TryLoadFunctionFromLibrary(void *library, const string &function_name) {
	return dlsym(library, function_name.c_str());
}

string ExtensionHelper::GetExtensionLibraryError() {
	return GetDLError();
}

} // namespace duckdb
