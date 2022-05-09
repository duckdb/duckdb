#ifndef UTIL_HH
#define UTIL_HH
#include <sstream>
#include <string>
#include <typeinfo>

using namespace std;

/* TODO: The strings are implementation-defined.  How do they look in
   clang? */

inline std::string pretty_type(const char *raw) {
	ostringstream os;
	os << raw;
	string s = os.str();
	while (s[0] <= '9')
		s.erase(s.begin());
	return s;
}

inline std::string pretty_type(struct prod *p) {
	return pretty_type(typeid(*p).name());
}

#endif
