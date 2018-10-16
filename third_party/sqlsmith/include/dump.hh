/// @file
/// @brief Dump syntax trees as GraphML
#ifndef DUMP_HH
#define DUMP_HH

#include <fstream>
#include <iostream>
#include <string>

#include "log.hh"
#include "prod.hh"

struct graphml_dumper : prod_visitor {
	std::ostream &o;
	virtual void visit(struct prod *p);
	graphml_dumper(std::ostream &out);
	std::string id(prod *p);
	std::string type(struct prod *p);
	virtual ~graphml_dumper();
};

struct ast_logger : logger {
	int queries = 0;
	virtual void generated(prod &query);
};

#endif
