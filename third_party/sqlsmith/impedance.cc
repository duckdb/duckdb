#include "impedance.hh"
#include "log.hh"
#include <iostream>

using namespace std;

static map<const char *, long> occurances_in_failed_query;
static map<const char *, long> occurances_in_ok_query;
static map<const char *, long> retries;
static map<const char *, long> limited;
static map<const char *, long> failed;

impedance_visitor::impedance_visitor(map<const char *, long> &occured) : _occured(occured) {
}

void impedance_visitor::visit(struct prod *p) {
	found[typeid(*p).name()] = true;
}

impedance_visitor::~impedance_visitor() {
	for (auto pair : found)
		_occured[pair.first]++;
}

void impedance_feedback::executed(prod &query) {
	impedance_visitor v(occurances_in_ok_query);
	query.accept(&v);
}

void impedance_feedback::error(prod &query, const dut::failure &e) {
	(void)e;
	impedance_visitor v(occurances_in_failed_query);
	query.accept(&v);
}

namespace impedance {

bool matched(const char *name) {
	if (100 > occurances_in_failed_query[name])
		return true;
	double error_rate =
	    (double)occurances_in_failed_query[name] / (occurances_in_failed_query[name] + occurances_in_ok_query[name]);
	if (error_rate > 0.99)
		return false;
	return true;
}

void report() {
	cerr << "impedance report: " << endl;
	for (auto pair : occurances_in_failed_query) {
		cerr << "  " << pretty_type(pair.first) << ": " << pair.second << "/" << occurances_in_ok_query[pair.first]
		     << " (bad/ok)";
		if (!matched(pair.first))
			cerr << " -> BLACKLISTED";
		cerr << endl;
	}
}

void report(std::ostream &out) {
	out << "{\"impedance\": [ " << endl;

	for (auto pair = occurances_in_failed_query.begin(); pair != occurances_in_failed_query.end(); ++pair) {
		out << "{\"prod\": \"" << pretty_type(pair->first) << "\","
		    << "\"bad\": " << pair->second << ", "
		    << "\"ok\": " << occurances_in_ok_query[pair->first] << ", "
		    << "\"limited\": " << limited[pair->first] << ", "
		    << "\"failed\": " << failed[pair->first] << ", "
		    << "\"retries\": " << retries[pair->first] << "} ";

		if (next(pair) != occurances_in_failed_query.end())
			out << "," << endl;
	}
	out << "]}" << endl;
}

void retry(const char *p) {
	retries[p]++;
}

void limit(const char *p) {
	limited[p]++;
}

void fail(const char *p) {
	failed[p]++;
}

} // namespace impedance
