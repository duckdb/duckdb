#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

bool ExtensionHelper::SupportsExternalExtensions() {
	return false;
}

void *ExtensionHelper::OpenExtensionLibrary(const string &filename, const string &filebase) {
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
}

void *ExtensionHelper::TryLoadFunctionFromLibrary(void *library, const string &function_name) {
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
}

string ExtensionHelper::GetExtensionLibraryError() {
	return string();
}

unique_ptr<ExtensionInstallInfo> ExtensionHelper::InstallExtensionInternal(DatabaseInstance &db, FileSystem &fs,
                                                                           const string &local_path,
                                                                           const string &extension,
                                                                           ExtensionInstallOptions &options,
                                                                           optional_ptr<ClientContext> context) {
	throw PermissionException("Installing external extensions is disabled through a compile time flag");
}

} // namespace duckdb
