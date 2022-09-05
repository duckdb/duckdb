#include <algorithm>
#include <cassert>
#include <numeric>
#include <stdexcept>
#include <typeinfo>

#include "random.hh"
#include "relmodel.hh"
#include "grammar.hh"
#include "schema.hh"
#include "impedance.hh"

using namespace std;

shared_ptr<table_ref> table_ref::factory(prod *p) {
	try {
		if (p->level < 3 + d6()) {
			if (d6() > 3 && p->level < d6())
				return make_shared<table_subquery>(p);
			if (d6() > 3)
				return make_shared<joined_table>(p);
		}
		if (d6() > 3)
			return make_shared<table_or_query_name>(p);
		else
			return make_shared<table_sample>(p);
	} catch (runtime_error &e) {
		p->retry();
	}
	return factory(p);
}

table_or_query_name::table_or_query_name(prod *p) : table_ref(p) {
	t = random_pick(scope->tables);
	refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("ref"), t));
}

void table_or_query_name::out(std::ostream &out) {
	out << t->ident() << " as " << refs[0]->ident();
}

target_table::target_table(prod *p, table *victim) : table_ref(p) {
	while (!victim || victim->schema == "pg_catalog" || !victim->is_base_table || !victim->columns().size()) {
		struct named_relation *pick = random_pick(scope->tables);
		victim = dynamic_cast<table *>(pick);
		retry();
	}
	victim_ = victim;
	refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("target"), victim));
}

void target_table::out(std::ostream &out) {
	out << victim_->ident() << " as " << refs[0]->ident();
}

table_sample::table_sample(prod *p) : table_ref(p) {
	match();
	retry_limit = 1000; /* retries are cheap here */
	do {
		auto pick = random_pick(scope->schema->base_tables);
		t = dynamic_cast<struct table *>(pick);
		retry();
	} while (!t || !t->is_base_table);

	refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("sample"), t));
	percent = 0.1 * d100();
	method = (d6() > 2) ? "system" : "bernoulli";
}

void table_sample::out(std::ostream &out) {
	out << t->ident() << " as " << refs[0]->ident() << " tablesample " << method << " (" << percent << ") ";
}

table_subquery::table_subquery(prod *p, bool lateral) : table_ref(p), is_lateral(lateral) {
	query = make_shared<query_spec>(this, scope, lateral);
	string alias = scope->stmt_uid("subq");
	relation *aliased_rel = &query->select_list->derived_table;
	refs.push_back(make_shared<aliased_relation>(alias, aliased_rel));
}

table_subquery::~table_subquery() {
}

void table_subquery::accept(prod_visitor *v) {
	query->accept(v);
	v->visit(this);
}

shared_ptr<join_cond> join_cond::factory(prod *p, table_ref &lhs, table_ref &rhs) {
	try {
		if (d6() < 6)
			return make_shared<expr_join_cond>(p, lhs, rhs);
		else
			return make_shared<simple_join_cond>(p, lhs, rhs);
	} catch (runtime_error &e) {
		p->retry();
	}
	return factory(p, lhs, rhs);
}

simple_join_cond::simple_join_cond(prod *p, table_ref &lhs, table_ref &rhs) : join_cond(p, lhs, rhs) {
retry:
	named_relation *left_rel = &*random_pick(lhs.refs);

	if (!left_rel->columns().size()) {
		retry();
		goto retry;
	}

	named_relation *right_rel = &*random_pick(rhs.refs);

	column &c1 = random_pick(left_rel->columns());

	for (auto c2 : right_rel->columns()) {
		if (c1.type == c2.type) {
			condition += left_rel->ident() + "." + c1.name + " = " + right_rel->ident() + "." + c2.name + " ";
			break;
		}
	}
	if (condition == "") {
		retry();
		goto retry;
	}
}

void simple_join_cond::out(std::ostream &out) {
	out << condition;
}

expr_join_cond::expr_join_cond(prod *p, table_ref &lhs, table_ref &rhs) : join_cond(p, lhs, rhs), joinscope(p->scope) {
	scope = &joinscope;
	for (auto ref : lhs.refs)
		joinscope.refs.push_back(&*ref);
	for (auto ref : rhs.refs)
		joinscope.refs.push_back(&*ref);
	search = bool_expr::factory(this);
}

void expr_join_cond::out(std::ostream &out) {
	out << *search;
}

joined_table::joined_table(prod *p) : table_ref(p) {
	lhs = table_ref::factory(this);
	rhs = table_ref::factory(this);

	condition = join_cond::factory(this, *lhs, *rhs);

	if (d6() < 4) {
		type = "inner";
	} else if (d6() < 4) {
		type = "left";
	} else {
		type = "right";
	}

	for (auto ref : lhs->refs)
		refs.push_back(ref);
	for (auto ref : rhs->refs)
		refs.push_back(ref);
}

void joined_table::out(std::ostream &out) {
	out << *lhs;
	indent(out);
	out << type << " join " << *rhs;
	indent(out);
	out << "on (" << *condition << ")";
}

void table_subquery::out(std::ostream &out) {
	if (is_lateral)
		out << "lateral ";
	out << "(" << *query << ") as " << refs[0]->ident();
}

void from_clause::out(std::ostream &out) {
	if (!reflist.size())
		return;
	out << "from ";

	for (auto r = reflist.begin(); r < reflist.end(); r++) {
		indent(out);
		out << **r;
		if (r + 1 != reflist.end())
			out << ",";
	}
}

from_clause::from_clause(prod *p) : prod(p) {
	reflist.push_back(table_ref::factory(this));
	for (auto r : reflist.back()->refs)
		scope->refs.push_back(&*r);

	while (d6() > 5) {
		// add a lateral subquery
		if (!impedance::matched(typeid(lateral_subquery)))
			break;
		reflist.push_back(make_shared<lateral_subquery>(this));
		for (auto r : reflist.back()->refs)
			scope->refs.push_back(&*r);
	}
}

select_list::select_list(prod *p) : prod(p) {
	do {
		shared_ptr<value_expr> e = value_expr::factory(this);
		value_exprs.push_back(e);
		ostringstream name;
		name << "c" << columns++;
		sqltype *t = e->type;
		assert(t);
		derived_table.columns().push_back(column(name.str(), t));
	} while (d6() > 1);
}

void select_list::out(std::ostream &out) {
	int i = 0;
	for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
		indent(out);
		out << **expr << " as " << derived_table.columns()[i].name;
		i++;
		if (expr + 1 != value_exprs.end())
			out << ", ";
	}
}

void query_spec::out(std::ostream &out) {
	out << "select " << set_quantifier << " " << *select_list;
	indent(out);
	out << *from_clause;
	indent(out);
	out << "where ";
	out << *search;
	if (limit_clause.length()) {
		indent(out);
		out << limit_clause;
	}
}

struct for_update_verify : prod_visitor {
	virtual void visit(prod *p) {
		if (dynamic_cast<window_function *>(p))
			throw("window function");
		joined_table *join = dynamic_cast<joined_table *>(p);
		if (join && join->type != "inner")
			throw("outer join");
		query_spec *subquery = dynamic_cast<query_spec *>(p);
		if (subquery)
			subquery->set_quantifier = "";
		table_or_query_name *tab = dynamic_cast<table_or_query_name *>(p);
		if (tab) {
			table *actual_table = dynamic_cast<table *>(tab->t);
			if (actual_table && !actual_table->is_insertable)
				throw("read only");
			if (actual_table->name.find("pg_"))
				throw("catalog");
		}
		table_sample *sample = dynamic_cast<table_sample *>(p);
		if (sample) {
			table *actual_table = dynamic_cast<table *>(sample->t);
			if (actual_table && !actual_table->is_insertable)
				throw("read only");
			if (actual_table->name.find("pg_"))
				throw("catalog");
		}
	};
};

select_for_update::select_for_update(prod *p, struct scope *s, bool lateral) : query_spec(p, s, lateral) {
	static const char *modes[] = {
	    "update",
	    "share",
	    "no key update",
	    "key share",
	};

	try {
		for_update_verify v1;
		this->accept(&v1);

	} catch (const char *reason) {
		lockmode = 0;
		return;
	}
	lockmode = modes[d6() % (sizeof(modes) / sizeof(*modes))];
	set_quantifier = ""; // disallow distinct
}

void select_for_update::out(std::ostream &out) {
	query_spec::out(out);
	if (lockmode) {
		indent(out);
		out << " for " << lockmode;
	}
}

query_spec::query_spec(prod *p, struct scope *s, bool lateral) : prod(p), myscope(s) {
	scope = &myscope;
	scope->tables = s->tables;

	if (lateral)
		scope->refs = s->refs;

	from_clause = make_shared<struct from_clause>(this);
	select_list = make_shared<struct select_list>(this);

	set_quantifier = (d100() == 1) ? "distinct" : "";

	search = bool_expr::factory(this);

	if (d6() > 2) {
		ostringstream cons;
		cons << "limit " << d100() + d100();
		limit_clause = cons.str();
	}
}

long prepare_stmt::seq;

void modifying_stmt::pick_victim() {
	do {
		struct named_relation *pick = random_pick(scope->tables);
		victim = dynamic_cast<struct table *>(pick);
		retry();
	} while (!victim || victim->schema == "pg_catalog" || !victim->is_base_table || !victim->columns().size());
}

modifying_stmt::modifying_stmt(prod *p, struct scope *s, table *victim) : prod(p), myscope(s) {
	scope = &myscope;
	scope->tables = s->tables;

	if (!victim)
		pick_victim();
}

delete_stmt::delete_stmt(prod *p, struct scope *s, table *v) : modifying_stmt(p, s, v) {
	scope->refs.push_back(victim);
	search = bool_expr::factory(this);
}

delete_returning::delete_returning(prod *p, struct scope *s, table *victim) : delete_stmt(p, s, victim) {
	match();
	select_list = make_shared<struct select_list>(this);
}

insert_stmt::insert_stmt(prod *p, struct scope *s, table *v) : modifying_stmt(p, s, v) {
	match();

	for (auto col : victim->columns()) {
		auto expr = value_expr::factory(this, col.type);
		assert(expr->type == col.type);
		value_exprs.push_back(expr);
	}
}

void insert_stmt::out(std::ostream &out) {
	out << "insert into " << victim->ident() << " ";

	if (!value_exprs.size()) {
		out << "default values";
		return;
	}

	out << "values (";

	for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
		indent(out);
		out << **expr;
		if (expr + 1 != value_exprs.end())
			out << ", ";
	}
	out << ")";
}

set_list::set_list(prod *p, table *target) : prod(p) {
	do {
		for (auto col : target->columns()) {
			if (d6() < 4)
				continue;
			auto expr = value_expr::factory(this, col.type);
			value_exprs.push_back(expr);
			names.push_back(col.name);
		}
	} while (!names.size());
}

void set_list::out(std::ostream &out) {
	assert(names.size());
	out << " set ";
	for (size_t i = 0; i < names.size(); i++) {
		indent(out);
		out << names[i] << " = " << *value_exprs[i];
		if (i + 1 != names.size())
			out << ", ";
	}
}

update_stmt::update_stmt(prod *p, struct scope *s, table *v) : modifying_stmt(p, s, v) {
	scope->refs.push_back(victim);
	search = bool_expr::factory(this);
	set_list = make_shared<struct set_list>(this, victim);
}

void update_stmt::out(std::ostream &out) {
	out << "update " << victim->ident() << *set_list;
}

update_returning::update_returning(prod *p, struct scope *s, table *v) : update_stmt(p, s, v) {
	match();

	select_list = make_shared<struct select_list>(this);
}

upsert_stmt::upsert_stmt(prod *p, struct scope *s, table *v) : insert_stmt(p, s, v) {
	match();

	if (!victim->constraints.size())
		fail("need table w/ constraint for upsert");

	set_list = std::make_shared<struct set_list>(this, victim);
	search = bool_expr::factory(this);
	constraint = random_pick(victim->constraints);
}

shared_ptr<prod> statement_factory(struct scope *s) {
	try {
		s->new_stmt();
		if (d42() == 1)
			return make_shared<merge_stmt>((struct prod *)0, s);
		if (d42() == 1)
			return make_shared<insert_stmt>((struct prod *)0, s);
		else if (d42() == 1)
			return make_shared<delete_returning>((struct prod *)0, s);
		else if (d42() == 1) {
			return make_shared<upsert_stmt>((struct prod *)0, s);
		} else if (d42() == 1)
			return make_shared<update_returning>((struct prod *)0, s);
		else if (d6() > 4)
			return make_shared<select_for_update>((struct prod *)0, s);
		else if (d6() > 5)
			return make_shared<common_table_expression>((struct prod *)0, s);
		return make_shared<query_spec>((struct prod *)0, s);
	} catch (runtime_error &e) {
		return statement_factory(s);
	}
}

void common_table_expression::accept(prod_visitor *v) {
	v->visit(this);
	for (auto q : with_queries)
		q->accept(v);
	query->accept(v);
}

common_table_expression::common_table_expression(prod *parent, struct scope *s) : prod(parent), myscope(s) {
	scope = &myscope;
	do {
		shared_ptr<query_spec> query = make_shared<query_spec>(this, s);
		with_queries.push_back(query);
		string alias = scope->stmt_uid("jennifer");
		relation *relation = &query->select_list->derived_table;
		auto aliased_rel = make_shared<aliased_relation>(alias, relation);
		refs.push_back(aliased_rel);
		scope->tables.push_back(&*aliased_rel);

	} while (d6() > 2);

retry:
	do {
		auto pick = random_pick(s->tables);
		scope->tables.push_back(pick);
	} while (d6() > 3);
	try {
		query = make_shared<query_spec>(this, scope);
	} catch (runtime_error &e) {
		retry();
		goto retry;
	}
}

void common_table_expression::out(std::ostream &out) {
	out << "WITH ";
	for (size_t i = 0; i < with_queries.size(); i++) {
		indent(out);
		out << refs[i]->ident() << " AS "
		    << "(" << *with_queries[i] << ")";
		if (i + 1 != with_queries.size())
			out << ", ";
		indent(out);
	}
	out << *query;
	indent(out);
}

merge_stmt::merge_stmt(prod *p, struct scope *s, table *v) : modifying_stmt(p, s, v) {
	match();
	target_table_ = make_shared<target_table>(this, victim);
	data_source = table_ref::factory(this);
	//   join_condition = join_cond::factory(this, *target_table_,
	//   *data_source);
	join_condition = make_shared<simple_join_cond>(this, *target_table_, *data_source);

	/* Put data_source into scope but not target_table.  Visibility of
	   the latter varies depending on kind of when clause. */
	//   for (auto r : data_source->refs)
	//     scope->refs.push_back(&*r);

	clauselist.push_back(when_clause::factory(this));
	while (d6() > 4)
		clauselist.push_back(when_clause::factory(this));
}

void merge_stmt::out(std::ostream &out) {
	out << "MERGE INTO " << *target_table_;
	indent(out);
	out << "USING " << *data_source;
	indent(out);
	out << "ON " << *join_condition;
	indent(out);
	for (auto p : clauselist) {
		out << *p;
		indent(out);
	}
}

void merge_stmt::accept(prod_visitor *v) {
	v->visit(this);
	target_table_->accept(v);
	data_source->accept(v);
	join_condition->accept(v);
	for (auto p : clauselist)
		p->accept(v);
}

when_clause::when_clause(merge_stmt *p) : prod(p) {
	condition = bool_expr::factory(this);
	matched = d6() > 3;
}

void when_clause::out(std::ostream &out) {
	out << (matched ? "WHEN MATCHED " : "WHEN NOT MATCHED");
	indent(out);
	out << "AND " << *condition;
	indent(out);
	out << " THEN ";
	out << (matched ? "DELETE" : "DO NOTHING");
}

void when_clause::accept(prod_visitor *v) {
	v->visit(this);
	condition->accept(v);
}

when_clause_update::when_clause_update(merge_stmt *p) : when_clause(p), myscope(p->scope) {
	myscope.tables = scope->tables;
	myscope.refs = scope->refs;
	scope = &myscope;
	scope->refs.push_back(&*(p->target_table_->refs[0]));

	set_list = std::make_shared<struct set_list>(this, p->victim);
}

void when_clause_update::out(std::ostream &out) {
	out << "WHEN MATCHED AND " << *condition;
	indent(out);
	out << " THEN UPDATE " << *set_list;
}

void when_clause_update::accept(prod_visitor *v) {
	v->visit(this);
	set_list->accept(v);
}

when_clause_insert::when_clause_insert(struct merge_stmt *p) : when_clause(p) {
	for (auto col : p->victim->columns()) {
		auto expr = value_expr::factory(this, col.type);
		assert(expr->type == col.type);
		exprs.push_back(expr);
	}
}

void when_clause_insert::out(std::ostream &out) {
	out << "WHEN NOT MATCHED AND " << *condition;
	indent(out);
	out << " THEN INSERT VALUES ( ";

	for (auto expr = exprs.begin(); expr != exprs.end(); expr++) {
		out << **expr;
		if (expr + 1 != exprs.end())
			out << ", ";
	}
	out << ")";
}

void when_clause_insert::accept(prod_visitor *v) {
	v->visit(this);
	for (auto p : exprs)
		p->accept(v);
}

shared_ptr<when_clause> when_clause::factory(struct merge_stmt *p) {
	try {
		switch (d6()) {
		case 1:
		case 2:
			return make_shared<when_clause_insert>(p);
		case 3:
		case 4:
			return make_shared<when_clause_update>(p);
		default:
			return make_shared<when_clause>(p);
		}
	} catch (runtime_error &e) {
		p->retry();
	}
	return factory(p);
}
