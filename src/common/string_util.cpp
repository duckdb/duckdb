
#include "common/string_util.hpp"

#include <algorithm>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdarg.h>
#include <string.h>
#include <string>

using namespace duckdb;

bool StringUtil::Contains(const std::string &haystack,
                          const std::string &needle) {
	return (haystack.find(needle) != std::string::npos);
}

/*
 * Remove trailing ' ', '\f', '\n', '\r', '\t', '\v'
 */
void StringUtil::RTrim(std::string &str) {
	str.erase(std::find_if(str.rbegin(), str.rend(),
	                       [](int ch) { return !std::isspace(ch); })
	              .base(),
	          str.end());
}

std::string StringUtil::Indent(int num_indent) {
	return std::string(num_indent, ' ');
}

bool StringUtil::StartsWith(const std::string &str, const std::string &prefix) {
	return std::equal(prefix.begin(), prefix.end(), str.begin());
}

bool StringUtil::EndsWith(const std::string &str, const std::string &suffix) {
	if (suffix.size() > str.size())
		return (false);
	return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

std::string StringUtil::Repeat(const std::string &str, const std::size_t n) {
	std::ostringstream os;
	if (n == 0 || str.empty()) {
		return (os.str());
	}
	for (int i = 0; i < static_cast<int>(n); i++) {
		os << str;
	}
	return (os.str());
}

std::vector<std::string> StringUtil::Split(const std::string &str,
                                           char delimiter) {
	std::stringstream ss(str);
	std::vector<std::string> lines;
	std::string temp;
	while (std::getline(ss, temp, delimiter)) {
		lines.push_back(temp);
	} // WHILE
	return (lines);
}

std::string StringUtil::Join(const std::vector<std::string> &input,
                             const std::string &separator) {
	// The result
	std::string result;

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

std::string StringUtil::Prefix(const std::string &str,
                               const std::string &prefix) {
	std::vector<std::string> lines = StringUtil::Split(str, '\n');
	if (lines.empty())
		return ("");

	std::ostringstream os;
	for (int i = 0, cnt = lines.size(); i < cnt; i++) {
		if (i > 0)
			os << std::endl;
		os << prefix << lines[i];
	} // FOR
	return (os.str());
}

std::string StringUtil::FormatSize(long bytes) {
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

std::string StringUtil::Bold(const std::string &str) {
	std::string SET_PLAIN_TEXT = "\033[0;0m";
	std::string SET_BOLD_TEXT = "\033[0;1m";

	std::ostringstream os;
	os << SET_BOLD_TEXT << str << SET_PLAIN_TEXT;
	return (os.str());
}

std::string StringUtil::Upper(const std::string &str) {
	std::string copy(str);
	std::transform(copy.begin(), copy.end(), copy.begin(),
	               [](unsigned char c) { return std::toupper(c); });
	return (copy);
}

std::string StringUtil::Lower(const std::string &str) {
	std::string copy(str);
	std::transform(copy.begin(), copy.end(), copy.begin(),
	               [](unsigned char c) { return std::tolower(c); });
	return (copy);
}

std::string StringUtil::Format(const std::string fmt_str, ...) {
	// Reserve two times as much as the length of the fmt_str
	int final_n, n = ((int)fmt_str.size()) * 2;
	std::string str;
	std::unique_ptr<char[]> formatted;
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
	return std::string(formatted.get());
}

std::string StringUtil::VFormat(const std::string fmt_str, va_list args) {
	va_list args_copy;

	std::unique_ptr<char[]> formatted;
	// make a copy of the args as we can only use it once
	va_copy(args_copy, args);

	// first get the amount of characters we need
	const auto n = vsnprintf(nullptr, 0, fmt_str.c_str(), args) + 1 ;

	// now allocate the string and do the actual printing
	formatted.reset(new char[n]);
	(void) vsnprintf(&formatted[0], n, fmt_str.c_str(), args_copy);
	return std::string(formatted.get());
}

std::vector<std::string> StringUtil::Split(const std::string &input,
                                           const std::string &split) {
	std::vector<std::string> splits;

	size_t last = 0;
	size_t input_len = input.size();
	size_t split_len = split.size();
	while (last <= input_len) {
		size_t next = input.find(split, last);
		if (next == std::string::npos) {
			next = input_len;
		}

		// Push the substring [last, next) on to splits
		std::string substr = input.substr(last, next - last);
		if (substr.empty() == false) {
			splits.push_back(substr);
		}
		last = next + split_len;
	}
	return splits;
}

std::string StringUtil::Strip(const std::string &str, char c) {
	// There's a copy here which is wasteful, so don't use this in performance
	// critical code!
	std::string tmp = str;
	tmp.erase(std::remove(tmp.begin(), tmp.end(), c), tmp.end());
	return tmp;
}
