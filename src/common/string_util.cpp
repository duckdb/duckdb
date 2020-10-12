#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdarg.h>
#include <string.h>

namespace duckdb {
using namespace std;

bool StringUtil::Contains(const string &haystack, const string &needle) {
	return (haystack.find(needle) != string::npos);
}

void StringUtil::LTrim(string &str) {
	auto it = str.begin();
	while (CharacterIsSpace(*it)) {
		it++;
	}
	str.erase(str.begin(), it);
}

// Remove trailing ' ', '\f', '\n', '\r', '\t', '\v'
void StringUtil::RTrim(string &str) {
	str.erase(find_if(str.rbegin(), str.rend(), [](int ch) { return ch > 0 && !CharacterIsSpace(ch); }).base(),
	          str.end());
}

void StringUtil::Trim(string &str) {
	StringUtil::LTrim(str);
	StringUtil::RTrim(str);
}

bool StringUtil::StartsWith(string str, string prefix) {
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

vector<string> StringUtil::TopNStrings(vector<std::pair<string, idx_t>> scores, idx_t n, idx_t threshold) {
	if (scores.size() == 0) {
		return vector<string>();
	}
	sort(scores.begin(), scores.end(),
	     [](const pair<string, idx_t> &a, const pair<string, idx_t> &b) -> bool { return a.second < b.second; });
	vector<string> result;
	result.push_back(scores[0].first);
	for (idx_t i = 1; i < MinValue<idx_t>(scores.size(), n); i++) {
		if (scores[i].second > threshold) {
			break;
		}
		result.push_back(scores[i].first);
	}
	return result;
}

struct LevenshteinArray {
	LevenshteinArray(idx_t len1, idx_t len2) : len1(len1) {
		dist = unique_ptr<idx_t[]>(new idx_t[len1 * len2]);
	}

	idx_t &score(idx_t i, idx_t j) {
		return dist[get_index(i, j)];
	}

private:
	idx_t len1;
	unique_ptr<idx_t[]> dist;

	idx_t get_index(idx_t i, idx_t j) {
		return j * len1 + i;
	}
};

// adapted from https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#C++
idx_t StringUtil::LevenshteinDistance(const string &s1, const string &s2) {
	idx_t len1 = s1.size();
	idx_t len2 = s2.size();
	if (len1 == 0) {
		return len2;
	}
	if (len2 == 0) {
		return len1;
	}
	LevenshteinArray array(len1 + 1, len2 + 1);
	array.score(0, 0) = 0;
	for (idx_t i = 0; i <= len1; i++) {
		array.score(i, 0) = i;
	}
	for (idx_t j = 0; j <= len2; j++) {
		array.score(0, j) = j;
	}
	for (idx_t i = 1; i <= len1; i++) {
		for (idx_t j = 1; j <= len2; j++) {
			// d[i][j] = std::min({ d[i - 1][j] + 1,
			//                      d[i][j - 1] + 1,
			//                      d[i - 1][j - 1] + (s1[i - 1] == s2[j - 1] ? 0 : 1) });
			int equal = s1[i - 1] == s2[j - 1] ? 0 : 1;
			idx_t adjacent_score1 = array.score(i - 1, j) + 1;
			idx_t adjacent_score2 = array.score(i, j - 1) + 1;
			idx_t adjacent_score3 = array.score(i - 1, j - 1) + equal;

			idx_t t = MinValue<idx_t>(adjacent_score1, adjacent_score2);
			array.score(i, j) = MinValue<idx_t>(t, adjacent_score3);
		}
	}
	return array.score(len1, len2);
}

vector<string> StringUtil::TopNLevenshtein(vector<string> strings, const string &target, idx_t n, idx_t threshold) {
	vector<std::pair<string, idx_t>> scores;
	for (auto &str : strings) {
		scores.push_back(make_pair(str, LevenshteinDistance(str, target)));
	}
	return TopNStrings(scores, n, threshold);
}

string StringUtil::CandidatesMessage(const vector<string> &candidates, string candidate) {
	string result_str;
	if (candidates.size() > 0) {
		result_str = "\n" + candidate + ": ";
		for (idx_t i = 0; i < candidates.size(); i++) {
			if (i > 0) {
				result_str += ", ";
			}
			result_str += "\"" + candidates[i] + "\"";
		}
	}
	return result_str;
}

} // namespace duckdb
