#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/common/types/value.hpp"

#include "utf8proc.hpp"

namespace duckdb {

// Helper function to generate column names
static string GenerateColumnName(const idx_t total_cols, const idx_t col_number, const string &prefix = "column") {
	auto max_digits = NumericHelper::UnsignedLength(total_cols - 1);
	auto digits = NumericHelper::UnsignedLength(col_number);
	string leading_zeros = string(NumericCast<idx_t>(max_digits - digits), '0');
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
		auto bytes = utf8proc_iterate(str + begin, NumericCast<utf8proc_ssize_t>(size - begin), &codepoint);
		D_ASSERT(bytes > 0);
		if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
			break;
		}
		begin += NumericCast<idx_t>(bytes);
	}

	// Find the last character that is not right trimmed
	idx_t end;
	end = begin;
	for (auto next = begin; next < col_name.size();) {
		auto bytes = utf8proc_iterate(str + next, NumericCast<utf8proc_ssize_t>(size - next), &codepoint);
		D_ASSERT(bytes > 0);
		next += NumericCast<idx_t>(bytes);
		if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
			end = next;
		}
	}

	// return the trimmed string
	return col_name.substr(begin, end - begin);
}

static string NormalizeColumnName(const string &col_name) {
	// normalize UTF8 characters to NFKD
	auto nfkd = utf8proc_NFKD(reinterpret_cast<const utf8proc_uint8_t *>(col_name.c_str()),
	                          NumericCast<utf8proc_ssize_t>(col_name.size()));
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

// If our columns were set by the user, we verify if their names match with the first row
bool CSVSniffer::DetectHeaderWithSetColumn() {
	bool has_header = true;
	bool all_varchar = true;
	bool first_row_consistent = true;
	std::ostringstream error;
	// User set the names, we must check if they match the first row
	// We do a +1 to check for situations where the csv file has an extra all null column
	if (set_columns.Size() != best_header_row.size() && set_columns.Size() + 1 != best_header_row.size()) {
		return false;
	} else {
		// Let's do a match-aroo
		for (idx_t i = 0; i < set_columns.Size(); i++) {
			if (best_header_row[i].IsNull()) {
				return false;
			}
			if (best_header_row[i].value.GetString() != (*set_columns.names)[i]) {
				error << "Header Mismatch at position:" << i << "\n";
				error << "Expected Name: \"" << (*set_columns.names)[i] << "\".";
				error << "Actual Name: \"" << best_header_row[i].value.GetString() << "\"."
				      << "\n";
				has_header = false;
				break;
			}
		}
	}
	if (!has_header) {
		// We verify if the types are consistent
		for (idx_t col = 0; col < set_columns.Size(); col++) {
			// try cast to sql_type of column
			const auto &sql_type = (*set_columns.types)[col];
			if (sql_type != LogicalType::VARCHAR) {
				all_varchar = false;
				if (!CanYouCastIt(best_header_row[col].value, sql_type, options.dialect_options,
				                  best_header_row[col].IsNull(), options.decimal_separator[0])) {
					first_row_consistent = false;
				}
			}
		}
		if (!first_row_consistent) {
			options.sniffer_user_mismatch_error += error.str();
		}
		if (all_varchar) {
			return true;
		}
		return !first_row_consistent;
	}
	return has_header;
}
void CSVSniffer::DetectHeader() {
	auto &sniffer_state_machine = best_candidate->GetStateMachine();
	if (best_header_row.empty()) {
		sniffer_state_machine.dialect_options.header = false;
		for (idx_t col = 0; col < sniffer_state_machine.dialect_options.num_cols; col++) {
			names.push_back(GenerateColumnName(sniffer_state_machine.dialect_options.num_cols, col));
		}
		// If the user provided names, we must replace our header with the user provided names
		if (!sniffer_state_machine.options.columns_set) {
			for (idx_t i = 0; i < MinValue<idx_t>(names.size(), sniffer_state_machine.options.name_list.size()); i++) {
				names[i] = sniffer_state_machine.options.name_list[i];
			}
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
	bool has_header;

	if (set_columns.IsSet()) {
		has_header = DetectHeaderWithSetColumn();
	} else {
		for (idx_t col = 0; col < best_header_row.size(); col++) {
			if (!best_header_row[col].IsNull()) {
				first_row_nulls = false;
			}
			// try cast to sql_type of column
			const auto &sql_type = best_sql_types_candidates_per_column_idx[col].back();
			if (sql_type != LogicalType::VARCHAR) {
				all_varchar = false;
				if (!CanYouCastIt(best_header_row[col].value, sql_type, sniffer_state_machine.dialect_options,
				                  best_header_row[col].IsNull(), options.decimal_separator[0])) {
					first_row_consistent = false;
				}
			}
		}
		// Our header is only false if types are not all varchar, and rows are consistent
		if (all_varchar || first_row_nulls) {
			has_header = true;
		} else {
			has_header = !first_row_consistent;
		}
	}

	if (sniffer_state_machine.options.dialect_options.header.IsSetByUser()) {
		// Header is defined by user, use that.
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
			string col_name = best_header_row[col].value.GetString();

			// generate name if field is empty
			if (col_name.empty() || best_header_row[col].IsNull()) {
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
	if (!sniffer_state_machine.options.columns_set) {
		for (idx_t i = 0; i < MinValue<idx_t>(names.size(), sniffer_state_machine.options.name_list.size()); i++) {
			names[i] = sniffer_state_machine.options.name_list[i];
		}
	}
}
} // namespace duckdb
