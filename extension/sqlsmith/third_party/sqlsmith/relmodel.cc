#include "relmodel.hh"
#include <algorithm>
#include <cctype>
#include <string>

map<string, sqltype *> sqltype::typemap;

sqltype *sqltype::get(string n) {
	std::transform(n.begin(), n.end(), n.begin(), [](unsigned char c) { return std::tolower(c); });

	if (typemap.count(n))
		return typemap[n];
	else
		return typemap[n] = new sqltype(n);
}

const map<string, sqltype *> &sqltype::get_types() {
	return typemap;
}

bool sqltype::consistent(struct sqltype *rvalue) {
	return this == rvalue;
}
