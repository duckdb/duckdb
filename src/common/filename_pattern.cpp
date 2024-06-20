#include "duckdb/common/filename_pattern.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void FilenamePattern::SetFilenamePattern(const string &pattern) {
	const string id_format {"{i}"};
	const string uuid_format {"{uuid}"};

	base = pattern;

	pos = base.find(id_format);
	uuid = false;
	if (pos != string::npos) {
		base = StringUtil::Replace(base, id_format, "");
		uuid = false;
	}

	pos = base.find(uuid_format);
	if (pos != string::npos) {
		base = StringUtil::Replace(base, uuid_format, "");
		uuid = true;
	}

	pos = std::min(pos, (idx_t)base.length());
}

string FilenamePattern::CreateFilename(FileSystem &fs, const string &path, const string &extension,
                                       idx_t offset) const {
	string result(base);
	string replacement;

	if (uuid) {
		replacement = UUID::ToString(UUID::GenerateRandomUUID());
	} else {
		replacement = std::to_string(offset);
	}
	result.insert(pos, replacement);
	return fs.JoinPath(path, result + "." + extension);
}

} // namespace duckdb
