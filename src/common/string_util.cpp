#include "common/string_util.hpp"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdarg.h>
#include <string.h>
#include <string>

using namespace duckdb;

bool StringUtil::Contains(const string &haystack, const string &needle) {
	return (haystack.find(needle) != string::npos);
}

/*
 * Remove trailing ' ', '\f', '\n', '\r', '\t', '\v'
 */
void StringUtil::RTrim(string &str) {
	str.erase(std::find_if(str.rbegin(), str.rend(), [](int ch) { return ch > 0 && !std::isspace(ch); }).base(),
	          str.end());
}

string StringUtil::Indent(int num_indent) {
	return string(num_indent, ' ');
}

bool StringUtil::StartsWith(const string &str, const string &prefix) {
	return std::equal(prefix.begin(), prefix.end(), str.begin());
}

bool StringUtil::EndsWith(const string &str, const string &suffix) {
	if (suffix.size() > str.size())
		return (false);
	return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

string StringUtil::Repeat(const string &str, const std::uint64_t n) {
	std::ostringstream os;
	if (n == 0 || str.empty()) {
		return (os.str());
	}
	for (int i = 0; i < static_cast<int>(n); i++) {
		os << str;
	}
	return (os.str());
}

vector<string> StringUtil::Split(const string &str, char delimiter) {
	std::stringstream ss(str);
	vector<string> lines;
	string temp;
	while (std::getline(ss, temp, delimiter)) {
		lines.push_back(temp);
	} // WHILE
	return (lines);
}

string StringUtil::Join(const vector<string> &input, const string &separator) {
	// The result
	string result;

	// If the input isn't empty, append the first element. We do this so we
	// don't
	// need to introduce an if into the loop.
	if (!input.empty()) {
		result += input[0];
	}

	// Append the remaining input components, after the first
	for (uint32_t i = 1; i < input.size(); i++) {
		result += separator + input[i];
	}

	return result;
}

string StringUtil::Prefix(const string &str, const string &prefix) {
	vector<string> lines = StringUtil::Split(str, '\n');
	if (lines.empty())
		return ("");

	std::ostringstream os;
	for (uint64_t i = 0, cnt = lines.size(); i < cnt; i++) {
		if (i > 0)
			os << std::endl;
		os << prefix << lines[i];
	} // FOR
	return (os.str());
}

string StringUtil::FormatSize(long bytes) {
	double BASE = 1024;
	double KB = BASE;
	double MB = KB * BASE;
	double GB = MB * BASE;

	std::ostringstream os;

	if (bytes >= GB) {
		os << std::fixed << std::setprecision(2) << (bytes / GB) << " GB";
	} else if (bytes >= MB) {
		os << std::fixed << std::setprecision(2) << (bytes / MB) << " MB";
	} else if (bytes >= KB) {
		os << std::fixed << std::setprecision(2) << (bytes / KB) << " KB";
	} else {
		os << std::to_string(bytes) + " bytes";
	}
	return (os.str());
}

string StringUtil::Bold(const string &str) {
	string SET_PLAIN_TEXT = "\033[0;0m";
	string SET_BOLD_TEXT = "\033[0;1m";

	std::ostringstream os;
	os << SET_BOLD_TEXT << str << SET_PLAIN_TEXT;
	return (os.str());
}

string StringUtil::Upper(const string &str) {
	string copy(str);
	std::transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::toupper(c); });
	return (copy);
}

string StringUtil::Lower(const string &str) {
	string copy(str);
	std::transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::tolower(c); });
	return (copy);
}

string StringUtil::Format(const string fmt_str, ...) {
	// Reserve two times as much as the length of the fmt_str
	int final_n, n = ((int)fmt_str.size()) * 2;
	string str;
	unique_ptr<char[]> formatted;
	va_list ap;

	while (1) {
		// Wrap the plain char array into the unique_ptr
		formatted.reset(new char[n + 1]);
		strcpy(&formatted[0], fmt_str.c_str());
		va_start(ap, fmt_str);
		final_n = vsnprintf(&formatted[0], n, fmt_str.c_str(), ap);
		va_end(ap);
		if (final_n < 0 || final_n >= n)
			n += abs(final_n - n + 1);
		else
			break;
	}
	return string(formatted.get());
}

string StringUtil::VFormat(const string fmt_str, va_list args) {
	va_list args_copy;

	unique_ptr<char[]> formatted;
	// make a copy of the args as we can only use it once
	va_copy(args_copy, args);

	// first get the amount of characters we need
	const auto n = vsnprintf(nullptr, 0, fmt_str.c_str(), args) + 1;

	// now allocate the string and do the actual printing
	formatted.reset(new char[n]);
	(void)vsnprintf(&formatted[0], n, fmt_str.c_str(), args_copy);
	return string(formatted.get());
}

vector<string> StringUtil::Split(const string &input, const string &split) {
	vector<string> splits;

	uint64_t last = 0;
	uint64_t input_len = input.size();
	uint64_t split_len = split.size();
	while (last <= input_len) {
		uint64_t next = input.find(split, last);
		if (next == string::npos) {
			next = input_len;
		}

		// Push the substring [last, next) on to splits
		string substr = input.substr(last, next - last);
		if (substr.empty() == false) {
			splits.push_back(substr);
		}
		last = next + split_len;
	}
	return splits;
}

string StringUtil::Strip(const string &str, char c) {
	// There's a copy here which is wasteful, so don't use this in performance
	// critical code!
	string tmp = str;
	tmp.erase(std::remove(tmp.begin(), tmp.end(), c), tmp.end());
	return tmp;
}
