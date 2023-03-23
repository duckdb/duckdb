#include "duckdb/common/filename_format_creator.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void FilenameFormatCreator::SetFilenameFormat(const string &format) {
	const string id_format {"{i}"};
	const string uuid_format {"{uuid}"};

	_base = format;

	_pos = _base.find(id_format);
	if (_pos != string::npos) {
		_base = StringUtil::Replace(_base, id_format, "");
		_uuid = false;
	}

	_pos = _base.find(uuid_format);
	if (_pos != string::npos) {
		_base = StringUtil::Replace(_base, uuid_format, "");
		_uuid = true;
	}

	_pos = std::min(_pos, (idx_t)_base.length());
}

string FilenameFormatCreator::CreateFilename(const FileSystem &fs, const string &path, const string &extension,
                                             idx_t offset) const {
	string result(_base);
	string replacement;

	if (_uuid) {
		replacement = UUID::ToString(UUID::GenerateRandomUUID());
	} else {
		replacement = std::to_string(offset);
	}
	result.insert(_pos, replacement);
	return fs.JoinPath(path, result + "." + extension);
}

} // namespace duckdb
