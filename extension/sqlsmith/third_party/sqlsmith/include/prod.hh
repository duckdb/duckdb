/// @file
/// @brief Base class for grammar productions

#include <iostream>
#include <string>

#ifndef PROD_HH
#define PROD_HH

/// Base class for walking the AST
struct prod_visitor {
	virtual void visit(struct prod *p) = 0;
	virtual ~prod_visitor() {
	}
};

/// Base class for AST nodes
struct prod {
	/// Parent production that instanciated this one.  May be NULL for
	/// top-level productions.
	struct prod *pprod;
	/// Scope object to model column/table reference visibility.
	struct scope *scope;
	/// Level of this production in the AST.  0 for root node.
	int level;
	/// Number of retries in this production.  Child productions are
	/// generated speculatively and may fail.
	long retries = 0;
	/// Maximum number of retries allowed before reporting a failure to
	/// the Parent prod.
	long retry_limit = 100;
	prod(prod *parent);
	virtual ~prod() {
	}
	/// Newline and indent according to tree level.
	virtual void indent(std::ostream &out);
	/// Emit SQL for this production.
	virtual void out(std::ostream &out) = 0;
	/// Check with the impedance matching code whether this production
	/// has been blacklisted and throw an exception.
	virtual void match();
	/// Visitor pattern for walking the AST.  Make sure you visit all
	/// child production when deriving classes.
	virtual void accept(prod_visitor *v) {
		v->visit(this);
	}
	/// Report a "failed to generate" error.
	virtual void fail(const char *reason);
	/// Increase the retry count and throw an exception when retry_limit
	/// is exceeded.
	void retry();
};

inline std::ostream &operator<<(std::ostream &s, prod &p) {
	p.out(s);
	return s;
}

#endif
