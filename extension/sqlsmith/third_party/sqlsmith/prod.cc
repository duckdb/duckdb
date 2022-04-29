/// @file
/// @brief Base class for grammar productions
#include <typeinfo>
#include <stdexcept>
#include "prod.hh"
#include "impedance.hh"

prod::prod(struct prod *parent) : pprod(parent) {
	if (parent) {
		level = parent->level + 1;
		scope = parent->scope;
	} else {
		scope = 0;
		level = 0;
	}
}

void prod::indent(std::ostream &out) {
	out << std::endl;
	for (int i = 0; i < level; i++)
		out << "  ";
}

void prod::retry() {
	impedance::retry(this);
	if (retries++ <= retry_limit)
		return;

	impedance::limit(this);
	throw std::runtime_error(std::string("excessive retries in ") + typeid(*this).name());
}

void prod::match() {
	if (!impedance::matched(this))
		throw std::runtime_error("impedance mismatch");
}

void prod::fail(const char *reason) {
	impedance::fail(this);
	throw std::runtime_error(reason);
}
