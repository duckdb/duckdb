#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

StorageOptions AttachInfo::GetStorageOptions() const {
	StorageOptions storage_options;
	for (auto &entry : options) {
		if (entry.first == "block_size") {
			// Extract the block allocation size. This is NOT the actual memory available on a block (block_size),
			// even though the corresponding option we expose to the user is called "block_size".
			storage_options.block_alloc_size = entry.second.GetValue<uint64_t>();
		} else if (entry.first == "row_group_size") {
			storage_options.row_group_size = entry.second.GetValue<uint64_t>();
		} else if (entry.first == "storage_version") {
			storage_options.storage_version =
			    SerializationCompatibility::FromString(entry.second.ToString()).serialization_version;
		}
	}
	return storage_options;
}

unique_ptr<AttachInfo> AttachInfo::Copy() const {
	auto result = make_uniq<AttachInfo>();
	result->name = name;
	result->path = path;
	result->options = options;
	result->on_conflict = on_conflict;
	return result;
}

string AttachInfo::ToString() const {
	string result = "";
	result += "ATTACH";
	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		result += " IF NOT EXISTS";
	}
	result += " DATABASE";
	result += KeywordHelper::WriteQuoted(path, '\'');
	if (!name.empty()) {
		result += " AS " + KeywordHelper::WriteOptionallyQuoted(name);
	}
	if (!options.empty()) {
		vector<string> stringified;
		for (auto &opt : options) {
			stringified.push_back(StringUtil::Format("%s %s", opt.first, opt.second.ToSQLString()));
		}
		result += " (" + StringUtil::Join(stringified, ", ") + ")";
	}
	result += ";";
	return result;
}

} // namespace duckdb
