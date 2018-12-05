#include <iostream>
#include <sstream>

#include <regex>

#include <string>

extern "C" {
#include <unistd.h>
}

#include "log.hh"
#include "schema.hh"
#include "impedance.hh"
#include "random.hh"

using namespace std;

struct stats_visitor : prod_visitor {
	int nodes = 0;
	int maxlevel = 0;
	long retries = 0;
	map<const char *, long> production_stats;
	virtual void visit(struct prod *p) {
		nodes++;
		if (p->level > maxlevel)
			maxlevel = p->level;
		production_stats[typeid(*p).name()]++;
		retries += p->retries;
	}
	void report() {
		cerr << "production statistics" << endl;
		vector<pair<const char *, long>> report;
		for (auto p : production_stats)
			report.push_back(p);
		stable_sort(
		    report.begin(), report.end(),
		    [](const pair<std::string, long> &a, const pair<std::string, long> &b) { return a.second > b.second; });
		for (auto p : report) {
			cerr << p.second << "\t" << p.first << endl;
		}
	}
};

void stats_collecting_logger::generated(prod &query) {
	queries++;

	stats_visitor v;
	query.accept(&v);

	sum_nodes += v.nodes;
	sum_height += v.maxlevel;
	sum_retries += v.retries;
}

void cerr_logger::report() {
	cerr << endl << "queries: " << queries << endl;
	// 	 << " (" << 1000.0*query_count/gen_time.count() << " gen/s, "
	// 	 << 1000.0*query_count/query_time.count() << " exec/s)" << endl;
	cerr << "AST stats (avg): height = " << sum_height / queries << " nodes = " << sum_nodes / queries << endl;

	vector<pair<std::string, long>> report;
	for (auto e : errors) {
		report.push_back(e);
	}
	stable_sort(report.begin(), report.end(),
	            [](const pair<std::string, long> &a, const pair<std::string, long> &b) { return a.second > b.second; });
	long err_count = 0;
	for (auto e : report) {
		err_count += e.second;
		cerr << e.second << "\t" << e.first.substr(0, 80) << endl;
	}
	cerr << "error rate: " << (float)err_count / (queries) << endl;
	impedance::report();
}

void cerr_logger::generated(prod &p) {
	stats_collecting_logger::generated(p);
	if ((10 * columns - 1) == queries % (10 * columns))
		report();
}

void cerr_logger::executed(prod &query) {
	(void)query;
	if (columns - 1 == (queries % columns)) {
		cerr << endl;
	}
	cerr << ".";
}

void cerr_logger::error(prod &query, const dut::failure &e) {
	(void)query;
	istringstream err(e.what());
	string line;

	if (columns - 1 == (queries % columns)) {
		cerr << endl;
	}
	getline(err, line);
	errors[line]++;
	if (dynamic_cast<const dut::timeout *>(&e))
		cerr << "t";
	else if (dynamic_cast<const dut::syntax *>(&e))
		cerr << "S";
	else if (dynamic_cast<const dut::broken *>(&e))
		cerr << "C";
	else
		cerr << "e";
}
