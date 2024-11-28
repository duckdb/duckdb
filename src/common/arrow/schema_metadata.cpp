#include "duckdb/common/arrow/schema_metadata.hpp"

namespace duckdb {

ArrowSchemaMetadata::ArrowSchemaMetadata(const char *metadata) {
	if (metadata) {
		// Read the number of key-value pairs (int32)
		int32_t num_pairs;
		memcpy(&num_pairs, metadata, sizeof(int32_t));
		metadata += sizeof(int32_t);

		// Loop through each key-value pair
		for (int32_t i = 0; i < num_pairs; ++i) {
			// Read the length of the key (int32)
			int32_t key_length;
			memcpy(&key_length, metadata, sizeof(int32_t));
			metadata += sizeof(int32_t);

			// Read the key
			std::string key(metadata, static_cast<idx_t>(key_length));
			metadata += key_length;

			// Read the length of the value (int32)
			int32_t value_length;
			memcpy(&value_length, metadata, sizeof(int32_t));
			metadata += sizeof(int32_t);

			// Read the value
			const std::string value(metadata, static_cast<idx_t>(value_length));
			metadata += value_length;
			schema_metadata_map[key] = value;
		}
	}
	extension_metadata_map = StringUtil::ParseJSONMap(schema_metadata_map[ARROW_METADATA_KEY]);
}

void ArrowSchemaMetadata::AddOption(const string &key, const string &value) {
	schema_metadata_map[key] = value;
}
string ArrowSchemaMetadata::GetOption(const string &key) const {
	auto it = schema_metadata_map.find(key);
	if (it != schema_metadata_map.end()) {
		return it->second;
	} else {
		return "";
	}
}

string ArrowSchemaMetadata::GetExtensionName() const {
	return GetOption(ARROW_EXTENSION_NAME);
}

ArrowSchemaMetadata ArrowSchemaMetadata::ArrowCanonicalType(const string &extension_name) {
	ArrowSchemaMetadata metadata;
	metadata.AddOption(ARROW_EXTENSION_NAME, extension_name);
	metadata.AddOption(ARROW_METADATA_KEY, "");
	return metadata;
}

ArrowSchemaMetadata ArrowSchemaMetadata::DuckDBInternalType(const string &type_name) {
	ArrowSchemaMetadata metadata;
	metadata.AddOption(ARROW_EXTENSION_NAME, ARROW_EXTENSION_NON_CANONICAL);
	// We have to set the metadata key with type_name and vendor_name.
	metadata.extension_metadata_map["vendor_name"] = "DuckDB";
	metadata.extension_metadata_map["type_name"] = type_name;
	metadata.AddOption(ARROW_METADATA_KEY, StringUtil::ToJSONMap(metadata.extension_metadata_map));
	return metadata;
}

bool ArrowSchemaMetadata::IsNonCanonicalType(const string &type, const string &vendor) const {
	if (schema_metadata_map.find(ARROW_EXTENSION_NAME) == schema_metadata_map.end()) {
		return false;
	}
	if (schema_metadata_map.find(ARROW_EXTENSION_NAME)->second != ARROW_EXTENSION_NON_CANONICAL) {
		return false;
	}
	if (extension_metadata_map.find("type_name") == extension_metadata_map.end() ||
	    extension_metadata_map.find("vendor_name") == extension_metadata_map.end()) {
		return false;
	}
	auto vendor_name = extension_metadata_map.find("vendor_name")->second;
	auto type_name = extension_metadata_map.find("type_name")->second;
	return vendor_name == vendor && type_name == type;
}

bool ArrowSchemaMetadata::HasExtension() const {
	auto arrow_extension = GetOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME);
	// FIXME: We are currently ignoring the ogc extensions
	return !arrow_extension.empty() && !StringUtil::StartsWith(arrow_extension, "ogc");
}

unsafe_unique_array<char> ArrowSchemaMetadata::SerializeMetadata() const {
	// First we have to figure out the total size:
	// 1. number of key-value pairs (int32)
	idx_t total_size = sizeof(int32_t);
	for (const auto &option : schema_metadata_map) {
		// 2. Length of the key and value (2 * int32)
		total_size += 2 * sizeof(int32_t);
		// 3. Length of key
		total_size += option.first.size();
		// 4. Length of value
		total_size += option.second.size();
	}
	auto metadata_array_ptr = make_unsafe_uniq_array<char>(total_size);
	auto metadata_ptr = metadata_array_ptr.get();
	// 1. number of key-value pairs (int32)
	const idx_t map_size = schema_metadata_map.size();
	memcpy(metadata_ptr, &map_size, sizeof(int32_t));
	metadata_ptr += sizeof(int32_t);
	// Iterate through each key-value pair in the map
	for (const auto &pair : schema_metadata_map) {
		const std::string &key = pair.first;
		idx_t key_size = key.size();
		// Length of the key (int32)
		memcpy(metadata_ptr, &key_size, sizeof(int32_t));
		metadata_ptr += sizeof(int32_t);
		// Key
		memcpy(metadata_ptr, key.c_str(), key_size);
		metadata_ptr += key_size;
		const std::string &value = pair.second;
		const idx_t value_size = value.size();
		// Length of the value (int32)
		memcpy(metadata_ptr, &value_size, sizeof(int32_t));
		metadata_ptr += sizeof(int32_t);
		// Value
		memcpy(metadata_ptr, value.c_str(), value_size);
		metadata_ptr += value_size;
	}
	return metadata_array_ptr;
}
} // namespace duckdb
