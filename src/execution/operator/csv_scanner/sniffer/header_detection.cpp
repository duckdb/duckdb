#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "utf8proc.hpp"

namespace duckdb {

// Helper function to generate column names
static string GenerateColumnName(const idx_t total_cols, const idx_t col_number, const string &prefix = "column") {
	int max_digits = NumericHelper::UnsignedLength(total_cols - 1);
	int digits = NumericHelper::UnsignedLength(col_number);
	string leading_zeros = string(max_digits - digits, '0');
	string value = to_string(col_number);
	return string(prefix + leading_zeros + value);
}

// Helper function for UTF-8 aware space trimming
static string TrimWhitespace(const string &col_name) {
	utf8proc_int32_t codepoint;
	auto str = reinterpret_cast<const utf8proc_uint8_t *>(col_name.c_str());
	idx_t size = col_name.size();
	// Find the first character that is not left trimmed
	idx_t begin = 0;
	while (begin < size) {
		auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
		D_ASSERT(bytes > 0);
		if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
			break;
		}
		begin += bytes;
	}

	// Find the last character that is not right trimmed
	idx_t end;
	end = begin;
	for (auto next = begin; next < col_name.size();) {
		auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
		D_ASSERT(bytes > 0);
		next += bytes;
		if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
			end = next;
		}
	}

	// return the trimmed string
	return col_name.substr(begin, end - begin);
}

static string NormalizeColumnName(const string &col_name) {
	// normalize UTF8 characters to NFKD
	auto nfkd = utf8proc_NFKD(reinterpret_cast<const utf8proc_uint8_t *>(col_name.c_str()), col_name.size());
	const string col_name_nfkd = string(const_char_ptr_cast(nfkd), strlen(const_char_ptr_cast(nfkd)));
	free(nfkd);

	// only keep ASCII characters 0-9 a-z A-Z and replace spaces with regular whitespace
	string col_name_ascii = "";
	for (idx_t i = 0; i < col_name_nfkd.size(); i++) {
		if (col_name_nfkd[i] == '_' || (col_name_nfkd[i] >= '0' && col_name_nfkd[i] <= '9') ||
		    (col_name_nfkd[i] >= 'A' && col_name_nfkd[i] <= 'Z') ||
		    (col_name_nfkd[i] >= 'a' && col_name_nfkd[i] <= 'z')) {
			col_name_ascii += col_name_nfkd[i];
		} else if (StringUtil::CharacterIsSpace(col_name_nfkd[i])) {
			col_name_ascii += " ";
		}
	}

	// trim whitespace and replace remaining whitespace by _
	string col_name_trimmed = TrimWhitespace(col_name_ascii);
	string col_name_cleaned = "";
	bool in_whitespace = false;
	for (idx_t i = 0; i < col_name_trimmed.size(); i++) {
		if (col_name_trimmed[i] == ' ') {
			if (!in_whitespace) {
				col_name_cleaned += "_";
				in_whitespace = true;
			}
		} else {
			col_name_cleaned += col_name_trimmed[i];
			in_whitespace = false;
		}
	}

	// don't leave string empty; if not empty, make lowercase
	if (col_name_cleaned.empty()) {
		col_name_cleaned = "_";
	} else {
		col_name_cleaned = StringUtil::Lower(col_name_cleaned);
	}

	// prepend _ if name starts with a digit or is a reserved keyword
	if (KeywordHelper::IsKeyword(col_name_cleaned) || (col_name_cleaned[0] >= '0' && col_name_cleaned[0] <= '9')) {
		col_name_cleaned = "_" + col_name_cleaned;
	}
	return col_name_cleaned;
}
void CSVSniffer::DetectHeader() {
	auto &sniffer_state_machine = best_candidate->GetStateMachine();

	if (best_header_row.empty()) {
		sniffer_state_machine.dialect_options.header = false;
		for (idx_t col = 0; col < sniffer_state_machine.dialect_options.num_cols; col++) {
			names.push_back(GenerateColumnName(sniffer_state_machine.dialect_options.num_cols, col));
		}
		// If the user provided names, we must replace our header with the user provided names
		for (idx_t i = 0; i < MinValue<idx_t>(names.size(), sniffer_state_machine.options.name_list.size()); i++) {
			names[i] = sniffer_state_machine.options.name_list[i];
		}
		return;
	}
	// information for header detection
	bool first_row_consistent = true;
	// check if header row is all null and/or consistent with detected column data types
	bool first_row_nulls = true;
	// If null-padding is not allowed and there is a mismatch between our header candidate and the number of columns
	// We can't detect the dialect/type options properly
	if (!sniffer_state_machine.options.null_padding &&
	    best_sql_types_candidates_per_column_idx.size() != best_header_row.size()) {
		auto error = CSVError::SniffingError(options.file_path);
		error_handler->Error(error);
	}
	bool all_varchar = true;
	for (idx_t col = 0; col < best_header_row.size(); col++) {
		auto dummy_val = best_header_row[col];
		if (!dummy_val.IsNull()) {
			first_row_nulls = false;
		}

		// try cast to sql_type of column
		const auto &sql_type = best_sql_types_candidates_per_column_idx[col].back();
		if (sql_type != LogicalType::VARCHAR) {
			all_varchar = false;
			if (!TryCastValue(sniffer_state_machine, dummy_val, sql_type)) {
				first_row_consistent = false;
			}
		}
	}
	bool has_header;
	if (!sniffer_state_machine.options.dialect_options.header.IsSetByUser()) {
		has_header = (!first_row_consistent || first_row_nulls) && !all_varchar;
	} else {
		has_header = sniffer_state_machine.options.dialect_options.header.GetValue();
	}
	// update parser info, and read, generate & set col_names based on previous findings
	if (has_header) {
		sniffer_state_machine.dialect_options.header = true;
		if (sniffer_state_machine.options.null_padding &&
		    !sniffer_state_machine.options.dialect_options.skip_rows.IsSetByUser()) {
			if (sniffer_state_machine.dialect_options.skip_rows.GetValue() > 0) {
				sniffer_state_machine.dialect_options.skip_rows =
				    sniffer_state_machine.dialect_options.skip_rows.GetValue() - 1;
			}
		}
		case_insensitive_map_t<idx_t> name_collision_count;

		// get header names from CSV
		for (idx_t col = 0; col < best_header_row.size(); col++) {
			const auto &val = best_header_row[col];
			string col_name = val.ToString();

			// generate name if field is empty
			if (col_name.empty() || val.IsNull()) {
				col_name = GenerateColumnName(sniffer_state_machine.dialect_options.num_cols, col);
			}

			// normalize names or at least trim whitespace
			if (sniffer_state_machine.options.normalize_names) {
				col_name = NormalizeColumnName(col_name);
			} else {
				col_name = TrimWhitespace(col_name);
			}

			// avoid duplicate header names
			while (name_collision_count.find(col_name) != name_collision_count.end()) {
				name_collision_count[col_name] += 1;
				col_name = col_name + "_" + to_string(name_collision_count[col_name]);
			}
			names.push_back(col_name);
			name_collision_count[col_name] = 0;
		}
		if (best_header_row.size() < sniffer_state_machine.dialect_options.num_cols && options.null_padding) {
			for (idx_t col = best_header_row.size(); col < sniffer_state_machine.dialect_options.num_cols; col++) {
				names.push_back(GenerateColumnName(sniffer_state_machine.dialect_options.num_cols, col));
			}
		} else if (best_header_row.size() < sniffer_state_machine.dialect_options.num_cols) {
			throw InternalException("Detected header has number of columns inferior to dialect detection");
		}

	} else {
		sniffer_state_machine.dialect_options.header = false;
		for (idx_t col = 0; col < sniffer_state_machine.dialect_options.num_cols; col++) {
			names.push_back(GenerateColumnName(sniffer_state_machine.dialect_options.num_cols, col));
		}
	}

	// If the user provided names, we must replace our header with the user provided names
	for (idx_t i = 0; i < MinValue<idx_t>(names.size(), sniffer_state_machine.options.name_list.size()); i++) {
		names[i] = sniffer_state_machine.options.name_list[i];
	}
}
} // namespace duckdb
