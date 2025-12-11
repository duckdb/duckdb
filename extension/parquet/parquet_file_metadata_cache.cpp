#include "parquet_file_metadata_cache.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

ParquetFileMetadataCache::ParquetFileMetadataCache(unique_ptr<duckdb_parquet::FileMetaData> file_metadata,
                                                   CachingFileHandle &handle,
                                                   unique_ptr<GeoParquetFileMetadata> geo_metadata,
                                                   unique_ptr<FileCryptoMetaData> crypto_metadata, idx_t footer_size)
    : metadata(std::move(file_metadata)), geo_metadata(std::move(geo_metadata)),
      crypto_metadata(std::move(crypto_metadata)), footer_size(footer_size), validate(handle.Validate()),
      last_modified(handle.GetLastModifiedTime()), version_tag(handle.GetVersionTag()) {
}

string ParquetFileMetadataCache::ObjectType() {
	return "parquet_metadata";
}

string ParquetFileMetadataCache::GetObjectType() {
	return ObjectType();
}

idx_t ParquetFileMetadataCache::GetRoughCacheMemory() const {
	// Base memory consumption
	idx_t memory = sizeof(*this);

	// Estimate metadata size - rough approximation
	if (metadata) {
		// Base metadata structure size + row groups + columns
		memory += 1024; // Base overhead
		// Estimate ~1KB per row group + column stats
		memory += metadata->row_groups.size() * 1024;
		// Column schema information
		memory += metadata->schema.size() * 256;
	}
	if (geo_metadata) {
		memory += 512;
	}

	if (crypto_metadata) {
		memory += 256;
	}

	memory += footer_size;
	memory += version_tag.size();

	return memory;
}

bool ParquetFileMetadataCache::IsValid(CachingFileHandle &new_handle) const {
	return ExternalFileCache::IsValid(validate, version_tag, last_modified, new_handle.GetVersionTag(),
	                                  new_handle.GetLastModifiedTime());
}

ParquetCacheValidity ParquetFileMetadataCache::IsValid(const OpenFileInfo &info) const {
	if (!info.extended_info) {
		return ParquetCacheValidity::UNKNOWN;
	}
	auto &open_options = info.extended_info->options;
	const auto validate_entry = open_options.find("validate_external_file_cache");
	if (validate_entry != open_options.end()) {
		// check if always valid - if so just return valid
		if (BooleanValue::Get(validate_entry->second)) {
			return ParquetCacheValidity::VALID;
		}
	}
	const auto lm_entry = open_options.find("last_modified");
	if (lm_entry == open_options.end()) {
		return ParquetCacheValidity::UNKNOWN;
	}
	auto new_last_modified = lm_entry->second.GetValue<timestamp_t>();
	string new_etag;
	const auto etag_entry = open_options.find("etag");
	if (etag_entry != open_options.end()) {
		new_etag = StringValue::Get(etag_entry->second);
	}
	if (ExternalFileCache::IsValid(false, version_tag, last_modified, new_etag, new_last_modified)) {
		return ParquetCacheValidity::VALID;
	}
	return ParquetCacheValidity::INVALID;
}

} // namespace duckdb
