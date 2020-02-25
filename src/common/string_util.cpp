#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdarg.h>
#include <string.h>
#include <string>

using namespace duckdb;
using namespace std;

bool StringUtil::Contains(const string &haystack, const string &needle) {
	return (haystack.find(needle) != string::npos);
}

void StringUtil::LTrim(string &str) {
	auto it = str.begin();
	while (isspace(*it)) {
		it++;
	}
	str.erase(str.begin(), it);
}

// Remove trailing ' ', '\f', '\n', '\r', '\t', '\v'
void StringUtil::RTrim(string &str) {
	str.erase(find_if(str.rbegin(), str.rend(), [](int ch) { return ch > 0 && !isspace(ch); }).base(), str.end());
}

void StringUtil::Trim(string &str) {
	StringUtil::LTrim(str);
	StringUtil::RTrim(str);
}

bool StringUtil::StartsWith(const string &str, const string &prefix) {
	if (prefix.size() > str.size()) {
		return false;
	}
	return equal(prefix.begin(), prefix.end(), str.begin());
}

bool StringUtil::EndsWith(const string &str, const string &suffix) {
	if (suffix.size() > str.size())
		return false;
	return equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

string StringUtil::Repeat(const string &str, idx_t n) {
	ostringstream os;
	if (n == 0 || str.empty()) {
		return (os.str());
	}
	for (int i = 0; i < static_cast<int>(n); i++) {
		os << str;
	}
	return (os.str());
}

vector<string> StringUtil::Split(const string &str, char delimiter) {
	stringstream ss(str);
	vector<string> lines;
	string temp;
	while (getline(ss, temp, delimiter)) {
		lines.push_back(temp);
	} // WHILE
	return (lines);
}

string StringUtil::Join(const vector<string> &input, const string &separator) {
	return StringUtil::Join(input, input.size(), separator, [](const string &s) { return s; });
}

string StringUtil::Prefix(const string &str, const string &prefix) {
	vector<string> lines = StringUtil::Split(str, '\n');
	if (lines.empty())
		return ("");

	ostringstream os;
	for (idx_t i = 0, cnt = lines.size(); i < cnt; i++) {
		if (i > 0)
			os << endl;
		os << prefix << lines[i];
	} // FOR
	return (os.str());
}

// http://ubuntuforums.org/showpost.php?p=10215516&postcount=5
string StringUtil::FormatSize(idx_t bytes) {
	double BASE = 1024;
	double KB = BASE;
	double MB = KB * BASE;
	double GB = MB * BASE;

	ostringstream os;

	if (bytes >= GB) {
		os << fixed << setprecision(2) << (bytes / GB) << " GB";
	} else if (bytes >= MB) {
		os << fixed << setprecision(2) << (bytes / MB) << " MB";
	} else if (bytes >= KB) {
		os << fixed << setprecision(2) << (bytes / KB) << " KB";
	} else {
		os << to_string(bytes) + " bytes";
	}
	return (os.str());
}

string StringUtil::Upper(const string &str) {
	string copy(str);
	transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return toupper(c); });
	return (copy);
}

string StringUtil::Lower(const string &str) {
	string copy(str);
	transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return tolower(c); });
	return (copy);
}

// http://stackoverflow.com/a/8098080
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

	idx_t last = 0;
	idx_t input_len = input.size();
	idx_t split_len = split.size();
	while (last <= input_len) {
		idx_t next = input.find(split, last);
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

string StringUtil::Replace(string source, const string &from, const string &to) {
	if (from.empty())
		return source;
	;
	idx_t start_pos = 0;
	while ((start_pos = source.find(from, start_pos)) != string::npos) {
		source.replace(start_pos, from.length(), to);
		start_pos += to.length(); // In case 'to' contains 'from', like
		                          // replacing 'x' with 'yx'
	}
	return source;
}
