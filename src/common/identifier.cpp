#include "duckdb/common/identifier.hpp"

#include "duckdb/common/string_util.hpp"

#include <ostream>

namespace duckdb {

hash_t Identifier::Hash() const {
	return StringUtil::CIHash(value);
}

bool operator==(const Identifier &a, const Identifier &b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), b.GetIdentifierName());
}
bool operator==(const Identifier &a, const string &b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), b);
}
bool operator==(const string &a, const Identifier &b) {
	return StringUtil::CIEquals(a, b.GetIdentifierName());
}
bool operator==(const Identifier &a, const char *b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), string(b));
}
bool operator==(const char *a, const Identifier &b) {
	return StringUtil::CIEquals(string(a), b.GetIdentifierName());
}

bool operator<(const Identifier &a, const Identifier &b) {
	return StringUtil::CILessThan(a.GetIdentifierName(), b.GetIdentifierName());
}

std::ostream &operator<<(std::ostream &os, const Identifier &id) {
	return os << id.GetIdentifierName();
}

} // namespace duckdb
