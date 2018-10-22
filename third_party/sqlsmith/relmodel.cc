#include "relmodel.hh"

map<string, sqltype *> sqltype::typemap;

sqltype *sqltype::get(string n) {
	if (typemap.count(n))
		return typemap[n];
	else
		return typemap[n] = new sqltype(n);
}

bool sqltype::consistent(struct sqltype *rvalue) {
	return this == rvalue;
}
