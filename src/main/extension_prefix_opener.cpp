#include "duckdb/main/replacement_opens.hpp"
#include "duckdb.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

struct ExtensionPrefixOpenData : public ReplacementOpenData {
	ExtensionPrefixOpenData(string extension, string path, unique_ptr<ReplacementOpenData> data)
	    : extension(move(extension)), path(move(path)), data(move(data)) {
	}
	string extension;
	string path;
	unique_ptr<ReplacementOpenData> data;
};

static unique_ptr<ReplacementOpenData> ExtensionPrefixPreOpen(DBConfig &config, ReplacementOpenStaticData *) {
	auto path = config.options.database_path;
	string extension = ExtensionHelper::ExtractExtensionPrefixFromPath(path);
	if(extension.empty()){
		return nullptr;
	}
	auto extension_data = ExtensionHelper::ReplacementOpenPre(extension, config);
	if (extension_data) {
		return make_unique<ExtensionPrefixOpenData>(extension, path, move(extension_data));
	}
	return nullptr;
}

static void ExtensionPrefixPostOpen(DatabaseInstance &instance, ReplacementOpenData *open_data) {
	if (!open_data) {
		return;
	}
	D_ASSERT(open_data);
	auto prefix_open_data = (ExtensionPrefixOpenData *)open_data;

	Connection con(instance);
	ExtensionHelper::LoadExternalExtension(*con.context, prefix_open_data->extension);
	ExtensionHelper::ReplacementOpenPost(*con.context, prefix_open_data->extension, instance,
	                                     prefix_open_data->data.get());
}

ExtensionPrefixReplacementOpen::ExtensionPrefixReplacementOpen()
    : ReplacementOpen(ExtensionPrefixPreOpen, ExtensionPrefixPostOpen) {
}

} // namespace duckdb
