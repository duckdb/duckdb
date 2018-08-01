
#include "common/types/operators.hpp"
#include "common/types/date.hpp"

#include <cstdlib>

using namespace duckdb;
using namespace std;

namespace operators {
template <> double Modulo::Operation(double left, double right) {
	throw NotImplementedException("Modulo for double not implemented!");
}

template <> int8_t Cast::Operation(const char *left) { return atoi(left); }

template <> int16_t Cast::Operation(const char *left) { return atoi(left); }

template <> int Cast::Operation(const char *left) { return atoi(left); }

template <> int64_t Cast::Operation(const char *left) { return atoll(left); }

template <> uint64_t Cast::Operation(const char *left) { return atoll(left); }

template <> double Cast::Operation(const char *left) { return atof(left); }

// numeric -> string
template <> const char *Cast::Operation(int8_t left) {
	throw NotImplementedException("String cast not implemented!");
}

template <> const char *Cast::Operation(int16_t left) {
	throw NotImplementedException("String cast not implemented!");
}

template <> const char *Cast::Operation(int left) {
	throw NotImplementedException("String cast not implemented!");
}

template <> const char *Cast::Operation(int64_t left) {
	throw NotImplementedException("String cast not implemented!");
}

template <> const char *Cast::Operation(uint64_t left) {
	throw NotImplementedException("String cast not implemented!");
}

template <> const char *Cast::Operation(double left) {
	throw NotImplementedException("String cast not implemented!");
}

template <> char *CastFromDate::Operation(date_t left) {
	auto conv = Date::ToString(left);
	auto ret = new char[conv.size() + 1];
	strcpy(ret, conv.c_str());
	return ret;
}

template <> date_t CastToDate::Operation(const char *left) {
	return Date::FromString(left);
}

} // namespace operators