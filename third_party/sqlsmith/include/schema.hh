/// @file
/// @brief Base class providing schema information to grammar

#ifndef SCHEMA_HH
#define SCHEMA_HH

#include <iostream>
#include <memory>
#include <numeric>
#include <string>

#include "random.hh"
#include "relmodel.hh"

struct schema {
	sqltype *booltype;
	sqltype *inttype;
	sqltype *internaltype;
	sqltype *arraytype;

	std::vector<sqltype *> types;

	std::vector<table> tables;
	std::vector<op> operators;
	std::vector<routine> routines;
	std::vector<routine> aggregates;

	typedef std::tuple<sqltype *, sqltype *, sqltype *> typekey;
	std::multimap<typekey, op> index;
	typedef std::multimap<typekey, op>::iterator op_iterator;

	std::multimap<sqltype *, routine *> routines_returning_type;
	std::multimap<sqltype *, routine *> aggregates_returning_type;
	std::multimap<sqltype *, routine *> parameterless_routines_returning_type;
	std::multimap<sqltype *, table *> tables_with_columns_of_type;
	std::multimap<sqltype *, op *> operators_returning_type;
	std::multimap<sqltype *, sqltype *> concrete_type;
	std::vector<table *> base_tables;

	string version;
	int version_num; // comparable version number

	const char *true_literal = "true";
	const char *false_literal = "false";

	virtual std::string quote_name(const std::string &id) = 0;

	void summary() {
		std::cout << "Found " << tables.size() << " user table(s) in information schema." << std::endl;
	}
	void fill_scope(struct scope &s) {
		for (auto &t : tables)
			s.tables.push_back(&t);
		s.schema = this;
	}
	virtual void register_operator(op &o) {
		operators.push_back(o);
		typekey t(o.left, o.right, o.result);
		index.insert(std::pair<typekey, op>(t, o));
	}
	virtual void register_routine(routine &r) {
		routines.push_back(r);
	}
	virtual void register_aggregate(routine &r) {
		aggregates.push_back(r);
	}
	virtual op_iterator find_operator(sqltype *left, sqltype *right, sqltype *res) {
		typekey t(left, right, res);
		auto cons = index.equal_range(t);
		if (cons.first == cons.second)
			return index.end();
		else
			return random_pick<>(cons.first, cons.second);
	}
	schema() {
	}
	virtual ~schema() {
	}
	void generate_indexes();
};

#endif
