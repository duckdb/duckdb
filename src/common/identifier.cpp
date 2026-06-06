#include "duckdb/common/identifier.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

hash_t Identifier::Hash() const {
	return StringUtil::CIHash(value);
}

bool operator==(const Identifier &a, const Identifier &b) {
	return StringUtil::CIEquals(a.GetName(), b.GetName());
}
bool operator==(const Identifier &a, const string &b) {
	return StringUtil::CIEquals(a.GetName(), b);
}
bool operator==(const string &a, const Identifier &b) {
	return StringUtil::CIEquals(a, b.GetName());
}
bool operator==(const Identifier &a, const char *b) {
	return StringUtil::CIEquals(a.GetName(), string(b));
}
bool operator==(const char *a, const Identifier &b) {
	return StringUtil::CIEquals(string(a), b.GetName());
}

bool operator<(const Identifier &a, const Identifier &b) {
	return StringUtil::CILessThan(a.GetName(), b.GetName());
}

} // namespace duckdb
