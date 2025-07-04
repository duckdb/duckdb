#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

StorageOptions AttachInfo::GetStorageOptions() const {
	StorageOptions storage_options;
	string storage_version_user_provided = "";
	for (auto &entry : options) {
		if (entry.first == "block_size") {
			// Extract the block allocation size. This is NOT the actual memory available on a block (block_size),
			// even though the corresponding option we expose to the user is called "block_size".
			storage_options.block_alloc_size = entry.second.GetValue<uint64_t>();
		} else if (entry.first == "encryption_key") {
			// check the type of the key
			auto type = entry.second.type();
			if (type.id() != LogicalTypeId::VARCHAR) {
				throw BinderException("\"%s\" is not a valid key. A key must be of type VARCHAR",
				                      entry.second.ToString());
			} else if (entry.second.GetValue<string>().empty()) {
				throw BinderException("Not a valid key. A key cannot be empty");
			}
			storage_options.user_key =
			    make_shared_ptr<string>(StringValue::Get(entry.second.DefaultCastAs(LogicalType::BLOB)));
			storage_options.block_header_size = DEFAULT_ENCRYPTION_BLOCK_HEADER_SIZE;
			storage_options.encryption = true;
		} else if (entry.first == "encryption_cipher") {
			throw BinderException("\"%s\" is not a valid cipher. Only AES GCM is supported.", entry.second.ToString());
		} else if (entry.first == "row_group_size") {
			storage_options.row_group_size = entry.second.GetValue<uint64_t>();
		} else if (entry.first == "storage_version") {
			storage_version_user_provided = entry.second.ToString();
			storage_options.storage_version =
			    SerializationCompatibility::FromString(entry.second.ToString()).serialization_version;
		}
	}
	if (storage_options.encryption && (!storage_options.storage_version.IsValid() ||
	                                   storage_options.storage_version.GetIndex() <
	                                       SerializationCompatibility::FromString("v1.4.0").serialization_version)) {
		if (!storage_version_user_provided.empty()) {
			throw InvalidInputException(
			    "Explicit provided STORAGE_VERSION (\"%s\") and ENCRYPTION_KEY (storage >= v1.4.0) are not compatible",
			    storage_version_user_provided);
		}
		// set storage version to v1.4.0
		storage_options.storage_version = SerializationCompatibility::FromString("v1.4.0").serialization_version;
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
	} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		result += " OR REPLACE";
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
