#include "duckdb/common/csv_utils.hpp"
#include "duckdb/common/serializer/write_stream.hpp"

namespace duckdb {

void CSVUtils::WriteQuoteOrEscape(WriteStream &writer, char quote_or_escape) {
	if (quote_or_escape != '\0') {
		writer.Write(quote_or_escape);
	}
}

string CSVUtils::AddEscapes(char to_be_escaped, char escape, const string &val) {
	idx_t i = 0;
	string new_val = "";
	idx_t found = val.find(to_be_escaped);

	while (found != string::npos) {
		while (i < found) {
			new_val += val[i];
			i++;
		}
		if (escape != '\0') {
			new_val += escape;
			found = val.find(to_be_escaped, found + 1);
		}
	}
	while (i < val.length()) {
		new_val += val[i];
		i++;
	}
	return new_val;
}

bool CSVUtils::RequiresQuotes(const char *str, idx_t len, vector<string> &null_str,
                              unsafe_unique_array<bool> &requires_quotes) {
	// check if the string is equal to the null string
	if (len == null_str[0].size() && memcmp(str, null_str[0].c_str(), len) == 0) {
		return true;
	}
	auto str_data = reinterpret_cast<const_data_ptr_t>(str);
	for (idx_t i = 0; i < len; i++) {
		if (requires_quotes[str_data[i]]) {
			// this byte requires quotes - write a quoted string
			return true;
		}
	}
	// no newline, quote or delimiter in the string
	// no quoting or escaping necessary
	return false;
}

void CSVUtils::WriteQuotedString(WriteStream &writer, const char *str, idx_t len, bool force_quote,
                                 vector<string> &null_str, unsafe_unique_array<bool> &requires_quotes, char quote,
                                 char escape) {
	if (!force_quote) {
		// force quote is disabled: check if we need to add quotes anyway
		force_quote = RequiresQuotes(str, len, null_str, requires_quotes);
	}
	// If a quote is set to none (i.e., null-terminator) we skip the quotation
	if (force_quote && quote != '\0') {
		// quoting is enabled: we might need to escape things in the string
		bool requires_escape = false;
		// simple CSV
		// do a single loop to check for a quote or escape value
		for (idx_t i = 0; i < len; i++) {
			if (str[i] == quote || str[i] == escape) {
				requires_escape = true;
				break;
			}
		}

		if (!requires_escape) {
			// fast path: no need to escape anything
			WriteQuoteOrEscape(writer, quote);
			writer.WriteData(const_data_ptr_cast(str), len);
			WriteQuoteOrEscape(writer, quote);
			return;
		}

		// slow path: need to add escapes
		string new_val(str, len);
		new_val = AddEscapes(escape, escape, new_val);
		if (escape != quote) {
			// need to escape quotes separately
			new_val = AddEscapes(quote, escape, new_val);
		}
		WriteQuoteOrEscape(writer, quote);
		writer.WriteData(const_data_ptr_cast(new_val.c_str()), new_val.size());
		WriteQuoteOrEscape(writer, quote);
	} else {
		writer.WriteData(const_data_ptr_cast(str), len);
	}
}

} // namespace duckdb
