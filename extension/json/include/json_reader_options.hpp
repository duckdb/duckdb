//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "json_common.hpp"
#include "json_enums.hpp"
#include "duckdb/common/types/type_map.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"

namespace duckdb {

struct DateFormatMap {
public:
	DateFormatMap() {
	}

	DateFormatMap(DateFormatMap &&other) noexcept {
		candidate_formats = other.candidate_formats;
	}
	DateFormatMap(const DateFormatMap &other) {
		candidate_formats = other.candidate_formats;
	}

	DateFormatMap &operator=(DateFormatMap &&other) noexcept {
		candidate_formats = other.candidate_formats;
		return *this;
	}

public:
	void Initialize(const type_id_map_t<vector<const char *>> &format_templates) {
		for (const auto &entry : format_templates) {
			const auto &type = entry.first;
			for (const auto &format_string : entry.second) {
				AddFormat(type, format_string);
			}
		}
	}

	DateFormatMap Copy() const {
		DateFormatMap result;
		result.candidate_formats = candidate_formats;
		return result;
	}

	void AddFormat(LogicalTypeId type, const string &format_string) {
		auto &formats = candidate_formats[type];
		formats.emplace_back();
		formats.back().format_specifier = format_string;
		StrpTimeFormat::ParseFormatSpecifier(formats.back().format_specifier, formats.back());
	}

	bool HasFormats(LogicalTypeId type) const {
		lock_guard<mutex> guard(lock);
		return candidate_formats.find(type) != candidate_formats.end();
	}

	idx_t NumberOfFormats(LogicalTypeId type) {
		lock_guard<mutex> guard(lock);
		return candidate_formats[type].size();
	}

	bool GetFormatAtIndex(LogicalTypeId type, idx_t index, StrpTimeFormat &format) {
		lock_guard<mutex> guard(lock);
		auto &formats = candidate_formats[type];
		if (index >= formats.size()) {
			return false;
		}
		format = formats[index];
		return true;
	}

	void ShrinkFormatsToSize(LogicalTypeId type, idx_t size) {
		lock_guard<mutex> guard(lock);
		auto &formats = candidate_formats[type];
		while (formats.size() > size) {
			formats.pop_back();
		}
	}

	StrpTimeFormat &GetFormat(LogicalTypeId type) {
		lock_guard<mutex> guard(lock);
		D_ASSERT(candidate_formats.find(type) != candidate_formats.end());
		return candidate_formats.find(type)->second.back();
	}

	const StrpTimeFormat &GetFormat(LogicalTypeId type) const {
		lock_guard<mutex> guard(lock);
		D_ASSERT(candidate_formats.find(type) != candidate_formats.end());
		return candidate_formats.find(type)->second.back();
	}

private:
	mutable mutex lock;
	type_id_map_t<vector<StrpTimeFormat>> candidate_formats;
};

struct JSONReaderOptions {
	//! The format of the JSON
	JSONFormat format = JSONFormat::AUTO_DETECT;
	//! Whether record types in the JSON
	JSONRecordType record_type = JSONRecordType::AUTO_DETECT;
	//! Whether file is compressed or not, and if so which compression type
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! Whether or not we should ignore malformed JSON (default to NULL)
	bool ignore_errors = false;
	//! Maximum JSON object size (defaults to 16MB minimum)
	idx_t maximum_object_size = 16777216;
	//! Whether we auto-detect a schema
	bool auto_detect = false;
	//! Sample size for detecting schema
	idx_t sample_size = idx_t(STANDARD_VECTOR_SIZE) * 10;
	//! Max depth we go to detect nested JSON schema (defaults to unlimited)
	idx_t max_depth = NumericLimits<idx_t>::Maximum();
	//! We divide the number of appearances of each JSON field by the auto-detection sample size
	//! If the average over the fields of an object is less than this threshold,
	//! we default to the MAP type with value type of merged field types
	double field_appearance_threshold = 0.1;
	//! The maximum number of files we sample to sample sample_size rows
	idx_t maximum_sample_files = 32;
	//! Whether we auto-detect and convert JSON strings to integers
	bool convert_strings_to_integers = false;
	//! If a struct contains more fields than this threshold with at least 80% similar types,
	//! we infer it as MAP type
	idx_t map_inference_threshold = 200;
	//! User-provided list of names (in order)
	vector<string> name_list;
	//! User-provided list of types (in order)
	vector<LogicalType> sql_type_list;
	//! Forced date/timestamp formats
	string date_format;
	string timestamp_format;
	//! Candidate date formats
	DateFormatMap date_format_map;
};

} // namespace duckdb
