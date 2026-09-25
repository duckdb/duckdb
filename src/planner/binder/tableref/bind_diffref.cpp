#include "duckdb/parser/tableref/diffref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// DIFF old, new [KEY (columns)] [CELLS]
//
// A semantic diff of two relations, after Hadley Wickham's data-diff (https://github.com/hadley/data-diff): the
// changes are reported as a small vocabulary of row and column operations rather than as a list of cells.
//
//   table_key([id], basis: guessed, overlap: 1.00)   the key rows were matched on, and how it was established
//   col_add(x) / col_drop(x)                         a column that exists on one side only
//   col_rename(old -> new, basis: exact)             a dropped and an added column holding the same values
//   col_edit(x, type: INTEGER -> BIGINT)             a column whose type changed
//   col_edit(x, changes: n)                          a column whose values changed in n matched rows
//   col_order(x, 3 -> 1)                             a column that moved
//   row_add(rows: n) / row_drop(rows: n)             rows that exist on one side only
//   row_edit(rows: n, changes: m, columns: [a, b])   rows sharing one set of changed columns
//   row_fanout(rows: n)                              old rows whose key matches several new rows
//   table_regenerate()                               the new relation is not usefully described as an edit
//
// The binder binds both sides once to learn their columns, decides the schema-level operations, and expands the
// rest into one SQL query over the two sides: the key is guessed from the data (the unique column combination
// shared by most rows), rows are matched on it, and the changed cells are summarized into the cheapest set of row
// and column edits, each priced by the bits needed to state it.
//===--------------------------------------------------------------------===//

namespace {

//! An old/new column pair with the same name
struct DiffColumn {
	string old_name;
	string new_name;
	idx_t old_index;
	idx_t new_index;
	LogicalType old_type;
	LogicalType new_type;
	//! Whether the values can be compared (equal types, or a common type both cast to)
	bool comparable;
	LogicalType cast_type;
};

//! A dropped/added column pair that might be one renamed column
struct DiffRenameCandidate {
	idx_t old_index;
	idx_t new_index;
	LogicalType cast_type;
};

//! Two identified columns whose contents might have been exchanged
struct DiffSwapCandidate {
	idx_t a; // indices into DiffSchema::common
	idx_t b;
	LogicalType cross_ab; // old a against new b
	LogicalType cross_ba; // old b against new a
};

//! A candidate key: a set of comparable column pairs
struct DiffKeyCandidate {
	vector<idx_t> columns; // indices into DiffSchema::common
};

struct DiffSide {
	vector<string> names;
	vector<LogicalType> types;
	string sql;
};

class DiffSQLGenerator {
public:
	DiffSQLGenerator(ClientContext &context, DiffRef &ref, DiffSide old_side, DiffSide new_side)
	    : context(context), ref(ref), old_side(std::move(old_side)), new_side(std::move(new_side)) {
	}

	string Generate();

private:
	void AnalyzeSchema();
	void EnumerateSwapCandidates();
	bool Comparable(const LogicalType &old_type, const LogicalType &new_type, LogicalType &cast_type) const;
	static string CastExpr(const string &expr, const LogicalType &from, const LogicalType &to);
	string InKey(idx_t column) const;
	void ResolveDeclaredKey();
	void EnumerateKeyCandidates();

	string OldExpr(const DiffColumn &col, const string &alias = "") const;
	string NewExpr(const DiffColumn &col, const string &alias = "") const;
	string KeyTuple(const DiffKeyCandidate &cand, bool old) const;
	string KeyList(const DiffKeyCandidate &cand, bool old) const;
	string DisplayList(const vector<idx_t> &columns) const;

	void GenerateSides(string &sql);
	void GenerateKey(string &sql);
	void GenerateMatching(string &sql);
	void GenerateRenames(string &sql);
	void GenerateCells(string &sql);
	void GenerateSummaryCTEs(string &sql);
	string GenerateSummaryResult();
	string GenerateCellsResult();

	//! Quote a column name for use in SQL
	static string Q(const string &name) {
		return KeywordHelper::WriteQuoted(name, '"');
	}
	//! A SQL string literal
	static string L(const string &str) {
		return "'" + StringUtil::Replace(str, "'", "''") + "'";
	}

private:
	ClientContext &context;
	DiffRef &ref;
	DiffSide old_side;
	DiffSide new_side;

	vector<DiffColumn> common;
	vector<idx_t> dropped; // indices into old_side
	vector<idx_t> added;   // indices into new_side
	vector<DiffRenameCandidate> rename_candidates;
	vector<DiffSwapCandidate> swap_candidates;
	vector<DiffKeyCandidate> key_candidates;
	//! The declared key (indices into common), empty when the key is guessed
	vector<idx_t> declared_key;
	//! Indices into common of the columns that take part in value comparison (comparable, not part of a declared key)
	vector<idx_t> value_columns;
};

void DiffSQLGenerator::AnalyzeSchema() {
	// pair up the columns by (case-insensitive) name
	case_insensitive_map_t<idx_t> new_columns;
	for (idx_t i = 0; i < new_side.names.size(); i++) {
		new_columns[new_side.names[i]] = i;
	}
	vector<bool> new_matched(new_side.names.size(), false);
	for (idx_t i = 0; i < old_side.names.size(); i++) {
		auto entry = new_columns.find(old_side.names[i]);
		if (entry == new_columns.end()) {
			dropped.push_back(i);
			continue;
		}
		DiffColumn col;
		col.old_name = old_side.names[i];
		col.new_name = new_side.names[entry->second];
		col.old_index = i;
		col.new_index = entry->second;
		col.old_type = old_side.types[i];
		col.new_type = new_side.types[entry->second];
		col.comparable = Comparable(col.old_type, col.new_type, col.cast_type);
		new_matched[entry->second] = true;
		common.push_back(std::move(col));
	}
	for (idx_t i = 0; i < new_side.names.size(); i++) {
		if (!new_matched[i]) {
			added.push_back(i);
		}
	}
	// a dropped and an added column of comparable types might be one renamed column
	static constexpr idx_t MAX_RENAME_SIDE = 8;
	for (idx_t d = 0; d < MinValue<idx_t>(dropped.size(), MAX_RENAME_SIDE); d++) {
		for (idx_t a = 0; a < MinValue<idx_t>(added.size(), MAX_RENAME_SIDE); a++) {
			DiffRenameCandidate cand;
			cand.old_index = dropped[d];
			cand.new_index = added[a];
			if (!Comparable(old_side.types[cand.old_index], new_side.types[cand.new_index], cand.cast_type)) {
				continue;
			}
			rename_candidates.push_back(std::move(cand));
		}
	}
}

static bool IsParsedString(const LogicalType &str, const LogicalType &other) {
	return str.id() == LogicalTypeId::VARCHAR && (other.IsNumeric() || other.IsTemporal());
}

//! Whether values of the two types can be compared, and the type both cast to: equal types as they are, a string
//! against a number, date or timestamp by parsing the string, everything else by the common type (if any)
bool DiffSQLGenerator::Comparable(const LogicalType &old_type, const LogicalType &new_type,
                                  LogicalType &cast_type) const {
	if (old_type == new_type) {
		cast_type = old_type;
		return true;
	}
	if (IsParsedString(old_type, new_type)) {
		cast_type = new_type;
		return true;
	}
	if (IsParsedString(new_type, old_type)) {
		cast_type = old_type;
		return true;
	}
	return LogicalType::TryGetMaxLogicalType(context, old_type, new_type, cast_type);
}

//! The expression cast to the comparison type; a string that does not parse compares as a changed value
string DiffSQLGenerator::CastExpr(const string &expr, const LogicalType &from, const LogicalType &to) {
	if (from == to) {
		return expr;
	}
	auto cast = from.id() == LogicalTypeId::VARCHAR ? "TRY_CAST(" : "CAST(";
	return cast + expr + " AS " + to.ToString() + ")";
}

//! Whether the column is part of the key (a runtime question for a guessed key)
string DiffSQLGenerator::InKey(idx_t column) const {
	if (!declared_key.empty()) {
		for (auto k : declared_key) {
			if (k == column) {
				return "true";
			}
		}
		return "false";
	}
	return "coalesce(list_contains((SELECT cols FROM __diff_key), " + L(common[column].new_name) + "), false)";
}

void DiffSQLGenerator::EnumerateSwapCandidates() {
	// two columns that are not part of the key, each comparable against the other's new values
	static constexpr idx_t MAX_SWAP_POOL = 8;
	vector<idx_t> pool;
	for (idx_t c = 0; c < common.size() && pool.size() < MAX_SWAP_POOL; c++) {
		bool in_declared_key = false;
		for (auto k : declared_key) {
			in_declared_key = in_declared_key || k == c;
		}
		if (!in_declared_key) {
			pool.push_back(c);
		}
	}
	for (idx_t i = 0; i < pool.size(); i++) {
		for (idx_t j = i + 1; j < pool.size(); j++) {
			DiffSwapCandidate cand;
			cand.a = pool[i];
			cand.b = pool[j];
			if (!Comparable(common[cand.a].old_type, common[cand.b].new_type, cand.cross_ab) ||
			    !Comparable(common[cand.b].old_type, common[cand.a].new_type, cand.cross_ba)) {
				continue;
			}
			swap_candidates.push_back(std::move(cand));
		}
	}
}

void DiffSQLGenerator::ResolveDeclaredKey() {
	for (auto &name : ref.key) {
		optional_idx found;
		for (idx_t i = 0; i < common.size(); i++) {
			if (StringUtil::CIEquals(common[i].old_name, name)) {
				found = i;
				break;
			}
		}
		if (!found.IsValid()) {
			throw BinderException("DIFF: key column \"%s\" must exist in both relations", name);
		}
		auto &col = common[found.GetIndex()];
		if (!col.comparable) {
			throw BinderException("DIFF: key column \"%s\" has incomparable types %s and %s", name,
			                      col.old_type.ToString(), col.new_type.ToString());
		}
		for (auto existing : declared_key) {
			if (existing == found.GetIndex()) {
				throw BinderException("DIFF: key column \"%s\" is listed twice", name);
			}
		}
		declared_key.push_back(found.GetIndex());
	}
}

void DiffSQLGenerator::EnumerateKeyCandidates() {
	if (!declared_key.empty()) {
		return;
	}
	// the pool: comparable columns, bounded so the candidate stats stay a handful of aggregates
	static constexpr idx_t MAX_KEY_POOL = 6;
	vector<idx_t> pool;
	for (idx_t i = 0; i < common.size() && pool.size() < MAX_KEY_POOL; i++) {
		if (common[i].comparable) {
			pool.push_back(i);
		}
	}
	for (auto c : pool) {
		key_candidates.push_back(DiffKeyCandidate {{c}});
	}
	for (idx_t i = 0; i < pool.size(); i++) {
		for (idx_t j = i + 1; j < pool.size(); j++) {
			key_candidates.push_back(DiffKeyCandidate {{pool[i], pool[j]}});
		}
	}
}

string DiffSQLGenerator::OldExpr(const DiffColumn &col, const string &alias) const {
	string expr = (alias.empty() ? "" : alias + ".") + Q(col.old_name);
	return col.comparable ? CastExpr(expr, col.old_type, col.cast_type) : expr;
}

string DiffSQLGenerator::NewExpr(const DiffColumn &col, const string &alias) const {
	string expr = (alias.empty() ? "" : alias + ".") + Q(col.new_name);
	return col.comparable ? CastExpr(expr, col.new_type, col.cast_type) : expr;
}

//! The candidate's columns as a comma-separated expression list (a row when there are several)
string DiffSQLGenerator::KeyTuple(const DiffKeyCandidate &cand, bool old) const {
	vector<string> exprs;
	for (auto c : cand.columns) {
		exprs.push_back(old ? OldExpr(common[c]) : NewExpr(common[c]));
	}
	if (exprs.size() == 1) {
		return exprs[0];
	}
	return "row(" + StringUtil::Join(exprs, ", ") + ")";
}

//! The candidate's columns as the VARCHAR list rows are matched on
string DiffSQLGenerator::KeyList(const DiffKeyCandidate &cand, bool old) const {
	vector<string> exprs;
	for (auto c : cand.columns) {
		exprs.push_back("CAST(" + (old ? OldExpr(common[c]) : NewExpr(common[c])) + " AS VARCHAR)");
	}
	return "[" + StringUtil::Join(exprs, ", ") + "]";
}

//! A SQL list literal of the names of the given common columns (under their new names)
string DiffSQLGenerator::DisplayList(const vector<idx_t> &columns) const {
	vector<string> names;
	for (auto c : columns) {
		names.push_back(L(common[c].new_name));
	}
	return "[" + StringUtil::Join(names, ", ") + "]::VARCHAR[]";
}

void DiffSQLGenerator::GenerateSides(string &sql) {
	sql += "WITH __diff_o AS (" + old_side.sql + "),\n";
	sql += "__diff_n AS (" + new_side.sql + "),\n";
}

void DiffSQLGenerator::GenerateKey(string &sql) {
	if (!declared_key.empty()) {
		DiffKeyCandidate key {declared_key};
		// a declared key must identify each old row; it may be duplicated in new (that is a fanout)
		sql += "__diff_keycheck AS (SELECT count(*) = count(DISTINCT " + KeyTuple(key, true) +
		       ") AS unique_o FROM __diff_o),\n";
		sql +=
		    "__diff_key AS (SELECT 0 AS id, " + DisplayList(declared_key) + " AS cols, " +
		    to_string(declared_key.size()) +
		    " AS width, CASE WHEN (SELECT unique_o FROM __diff_keycheck) THEN 'declared' ELSE error('DIFF: the "
		    "declared key does not identify each row of the old relation') END AS basis, NULL::DOUBLE AS overlap),\n";
		return;
	}
	// per-side stats of every candidate in one aggregate pass
	for (idx_t side = 0; side < 2; side++) {
		bool old = side == 0;
		sql += old ? "__diff_so AS (SELECT count(*) AS n" : "__diff_sn AS (SELECT count(*) AS n";
		for (idx_t k = 0; k < key_candidates.size(); k++) {
			auto &cand = key_candidates[k];
			vector<string> null_checks;
			for (auto c : cand.columns) {
				null_checks.push_back((old ? OldExpr(common[c]) : NewExpr(common[c])) + " IS NULL");
			}
			sql += StringUtil::Format(", count(DISTINCT %s) AS u%llu, coalesce(bool_or(%s), false) AS null%llu",
			                          KeyTuple(cand, old), k, StringUtil::Join(null_checks, " OR "), k);
		}
		sql += old ? " FROM __diff_o),\n" : " FROM __diff_n),\n";
	}
	// the candidates: unique in old, sharing at least one tuple with new, and duplicated in new only for a single
	// column and only for few (<= 10%) of the old rows - a compound candidate has an alternative (one more column)
	sql += "__diff_cand AS (SELECT * FROM (VALUES\n";
	for (idx_t k = 0; k < key_candidates.size(); k++) {
		auto &cand = key_candidates[k];
		vector<string> old_cols;
		vector<string> new_cols;
		for (auto c : cand.columns) {
			old_cols.push_back(OldExpr(common[c]));
			new_cols.push_back(NewExpr(common[c]));
		}
		string shared =
		    StringUtil::Format("(SELECT count(*) FROM (SELECT %s FROM __diff_o INTERSECT SELECT %s FROM __diff_n))",
		                       StringUtil::Join(old_cols, ", "), StringUtil::Join(new_cols, ", "));
		string dup_old = "0";
		if (cand.columns.size() == 1) {
			dup_old = StringUtil::Format(
			    "(SELECT count(*) FROM __diff_o WHERE %s IN (SELECT %s FROM __diff_n GROUP BY 1 HAVING count(*) > 1))",
			    old_cols[0], new_cols[0]);
		}
		sql += StringUtil::Format(
		    "  (%llu, %s, %llu, (SELECT u%llu = n AND NOT null%llu FROM __diff_so), (SELECT u%llu = n "
		    "AND NOT null%llu FROM __diff_sn), %s, %s, (SELECT n FROM __diff_so), (SELECT u%llu FROM "
		    "__diff_sn))%s\n",
		    k + 1, DisplayList(cand.columns), cand.columns.size(), k, k, k, k, shared, dup_old, k,
		    k + 1 < key_candidates.size() ? "," : "");
	}
	if (key_candidates.empty()) {
		sql += "  (NULL::BIGINT, NULL::VARCHAR[], NULL::BIGINT, false, false, 0::BIGINT, 0::BIGINT, 0::BIGINT, "
		       "0::BIGINT)\n";
	}
	sql += ") t(id, cols, width, unique_o, unique_n, shared, dup_old, n_o, distinct_n)";
	if (key_candidates.empty()) {
		sql += " WHERE false";
	}
	sql += "),\n";
	// rank by evidence, then parsimony: most shared tuples, fewest columns, no fanout, column order
	sql += "__diff_key AS (SELECT id, cols, width, 'guessed' AS basis, shared::DOUBLE / least(n_o, distinct_n) AS "
	       "overlap FROM __diff_cand WHERE unique_o AND shared > 0 AND (unique_n OR (width = 1 AND dup_old * 10 <= "
	       "n_o)) ORDER BY shared DESC, width ASC, unique_n DESC, id ASC LIMIT 1),\n";
}

void DiffSQLGenerator::GenerateMatching(string &sql) {
	// the key of each row as a VARCHAR list; without a key rows are matched by position
	for (idx_t side = 0; side < 2; side++) {
		bool old = side == 0;
		sql += old ? "__diff_ok AS (SELECT *, " : "__diff_nk AS (SELECT *, ";
		if (!declared_key.empty()) {
			sql += KeyList(DiffKeyCandidate {declared_key}, old);
		} else if (key_candidates.empty()) {
			sql += "[CAST(__diff_pos AS VARCHAR)]";
		} else {
			sql += "CASE (SELECT id FROM __diff_key)";
			for (idx_t k = 0; k < key_candidates.size(); k++) {
				sql += StringUtil::Format(" WHEN %llu THEN %s", k + 1, KeyList(key_candidates[k], old));
			}
			sql += " ELSE [CAST(__diff_pos AS VARCHAR)] END";
		}
		sql += " AS __diff_k FROM (SELECT *, row_number() OVER () AS __diff_pos FROM ";
		sql += old ? "__diff_o)),\n" : "__diff_n)),\n";
	}
	sql += "__diff_nc AS (SELECT __diff_k, count(*) AS cnt FROM __diff_nk GROUP BY __diff_k),\n";
	// the one-to-one matched rows, carrying every column that takes part in value comparison
	sql += "__diff_m AS (SELECT o.__diff_pos AS __diff_opos, n.__diff_pos AS __diff_npos, o.__diff_k AS __diff_k";
	for (auto c : value_columns) {
		sql += StringUtil::Format(", %s AS __diff_o%llu, %s AS __diff_n%llu", OldExpr(common[c], "o"), c,
		                          NewExpr(common[c], "n"), c);
	}
	for (idx_t r = 0; r < rename_candidates.size(); r++) {
		auto &cand = rename_candidates[r];
		auto old_expr =
		    CastExpr("o." + Q(old_side.names[cand.old_index]), old_side.types[cand.old_index], cand.cast_type);
		auto new_expr =
		    CastExpr("n." + Q(new_side.names[cand.new_index]), new_side.types[cand.new_index], cand.cast_type);
		sql += StringUtil::Format(", %s AS __diff_ro%llu, %s AS __diff_rn%llu", old_expr, r, new_expr, r);
	}
	for (idx_t p = 0; p < swap_candidates.size(); p++) {
		auto &cand = swap_candidates[p];
		auto &a = common[cand.a];
		auto &b = common[cand.b];
		sql +=
		    StringUtil::Format(", %s AS __diff_xo%llu, %s AS __diff_xn%llu, %s AS __diff_yo%llu, %s AS __diff_yn%llu",
		                       CastExpr("o." + Q(a.old_name), a.old_type, cand.cross_ab), p,
		                       CastExpr("n." + Q(b.new_name), b.new_type, cand.cross_ab), p,
		                       CastExpr("o." + Q(b.old_name), b.old_type, cand.cross_ba), p,
		                       CastExpr("n." + Q(a.new_name), a.new_type, cand.cross_ba), p);
	}
	sql += " FROM __diff_ok o JOIN __diff_nk n ON o.__diff_k = n.__diff_k JOIN __diff_nc c ON c.__diff_k = "
	       "o.__diff_k WHERE c.cnt = 1),\n";
	sql += "__diff_fan AS (SELECT count(*) AS groups, coalesce(sum(c.cnt), 0)::BIGINT AS new_rows FROM __diff_ok o "
	       "JOIN __diff_nc c ON c.__diff_k = o.__diff_k WHERE c.cnt > 1),\n";
	sql += "__diff_dropped AS (SELECT count(*) AS n FROM __diff_ok o WHERE NOT EXISTS (SELECT 1 FROM __diff_nk n "
	       "WHERE n.__diff_k = o.__diff_k)),\n";
	sql += "__diff_added AS (SELECT count(*) AS n FROM __diff_nk n WHERE NOT EXISTS (SELECT 1 FROM __diff_ok o "
	       "WHERE o.__diff_k = n.__diff_k)),\n";
}

void DiffSQLGenerator::GenerateRenames(string &sql) {
	// a dropped and an added column are one renamed column when they agree on (nearly) every matched row
	sql += "__diff_ren AS (SELECT count(*) AS matched";
	for (idx_t r = 0; r < rename_candidates.size(); r++) {
		sql += StringUtil::Format(
		    ", count(*) FILTER (WHERE __diff_ro%llu IS NOT DISTINCT FROM __diff_rn%llu) AS agree%llu", r, r, r);
	}
	sql += " FROM __diff_m),\n";
	sql += "__diff_renpairs AS (SELECT * FROM (VALUES\n";
	for (idx_t r = 0; r < rename_candidates.size(); r++) {
		auto &cand = rename_candidates[r];
		sql += StringUtil::Format(
		    "  (%llu, %s, %s, (SELECT agree%llu FROM __diff_ren), (SELECT matched FROM __diff_ren))%s\n", r,
		    L(old_side.names[cand.old_index]), L(new_side.names[cand.new_index]), r,
		    r + 1 < rename_candidates.size() ? "," : "");
	}
	if (rename_candidates.empty()) {
		sql += "  (NULL::BIGINT, NULL::VARCHAR, NULL::VARCHAR, 0::BIGINT, 0::BIGINT)\n";
	}
	sql += ") t(id, old_col, new_col, agree, matched)";
	if (rename_candidates.empty()) {
		sql += " WHERE false";
	}
	sql += "),\n";
	// exact agreement, or fewer than 10% of the values differing; each column pairs with its mutual best match
	sql += "__diff_renames AS (SELECT id, old_col, new_col, CASE WHEN agree = matched THEN 'exact' ELSE 'approximate' "
	       "END AS basis, matched - agree AS changes FROM __diff_renpairs WHERE matched > 0 AND agree * 10 >= matched "
	       "* 9 QUALIFY row_number() OVER (PARTITION BY old_col ORDER BY agree DESC, id) = 1 AND row_number() OVER "
	       "(PARTITION BY new_col ORDER BY agree DESC, id) = 1),\n";
	// two columns were exchanged when each changed under its own name in most rows while each holds (nearly) all
	// of what the other held; each column takes part in at most one exchange, the best-agreeing one
	sql += "__diff_swapstats AS (SELECT count(*) AS matched";
	for (idx_t p = 0; p < swap_candidates.size(); p++) {
		auto &cand = swap_candidates[p];
		auto own = [&](idx_t c) {
			return common[c].comparable ? StringUtil::Format("__diff_o%llu IS DISTINCT FROM __diff_n%llu", c, c)
			                            : string("true");
		};
		sql += StringUtil::Format(", count(*) FILTER (WHERE __diff_xo%llu IS NOT DISTINCT FROM __diff_xn%llu) AS "
		                          "ab%llu, count(*) FILTER (WHERE __diff_yo%llu IS NOT DISTINCT FROM __diff_yn%llu) AS "
		                          "ba%llu, count(*) FILTER (WHERE %s) AS oa%llu, count(*) FILTER (WHERE %s) AS ob%llu",
		                          p, p, p, p, p, p, own(cand.a), p, own(cand.b), p);
	}
	sql += " FROM __diff_m),\n";
	sql += "__diff_swappairs AS (SELECT * FROM (VALUES\n";
	for (idx_t p = 0; p < swap_candidates.size(); p++) {
		auto &cand = swap_candidates[p];
		auto &a = common[cand.a];
		auto &b = common[cand.b];
		// the exchange moves old b to new a's position, which is the move ordering reports
		sql += StringUtil::Format("  (%llu, %s, %s, (SELECT ab%llu FROM __diff_swapstats), (SELECT ba%llu FROM "
		                          "__diff_swapstats), (SELECT oa%llu FROM __diff_swapstats), (SELECT ob%llu FROM "
		                          "__diff_swapstats), (SELECT matched FROM __diff_swapstats), %llu, %llu, %s)%s\n",
		                          p, L(a.new_name), L(b.new_name), p, p, p, p, b.old_index + 1, a.new_index + 1,
		                          InKey(cand.a) + " OR " + InKey(cand.b), p + 1 < swap_candidates.size() ? "," : "");
	}
	if (swap_candidates.empty()) {
		sql += "  (NULL::BIGINT, NULL::VARCHAR, NULL::VARCHAR, 0::BIGINT, 0::BIGINT, 0::BIGINT, 0::BIGINT, "
		       "0::BIGINT, 0::BIGINT, 0::BIGINT, false)\n";
	}
	sql += ") t(id, a, b, ab, ba, oa, ob, matched, old_position, new_position, in_key)";
	if (swap_candidates.empty()) {
		sql += " WHERE false";
	}
	sql += "),\n";
	sql += "__diff_swapq AS (SELECT * FROM __diff_swappairs WHERE NOT in_key AND matched > 0 AND ab * 10 >= matched "
	       "* 9 AND ba * 10 >= matched * 9 AND oa * 2 > matched AND ob * 2 > matched),\n";
	sql += "__diff_swaps AS (SELECT q.* FROM __diff_swapq q WHERE NOT EXISTS (SELECT 1 FROM __diff_swapq o WHERE "
	       "o.id <> q.id AND (o.a IN (q.a, q.b) OR o.b IN (q.a, q.b)) AND (o.ab + o.ba > q.ab + q.ba OR (o.ab + o.ba "
	       "= q.ab + q.ba AND o.id < q.id)))),\n";
}

void DiffSQLGenerator::GenerateCells(string &sql) {
	// per matched row: the names of the identified columns whose value changed, in the new relation's column order;
	// key columns identify rows rather than being edited, so they are left out
	sql += "__diff_cells AS (SELECT __diff_opos, __diff_npos, __diff_k, list_filter([";
	vector<std::pair<idx_t, string>> entries; // (new-side position, entry)
	for (auto c : value_columns) {
		auto name = L(common[c].new_name);
		entries.emplace_back(
		    common[c].new_index,
		    StringUtil::Format("CASE WHEN NOT %s AND NOT EXISTS (SELECT 1 FROM __diff_swaps WHERE a = "
		                       "%s OR b = %s) AND __diff_o%llu IS DISTINCT FROM __diff_n%llu THEN %s END",
		                       InKey(c), name, name, c, c, name));
	}
	for (idx_t r = 0; r < rename_candidates.size(); r++) {
		auto new_index = rename_candidates[r].new_index;
		entries.emplace_back(new_index,
		                     StringUtil::Format("CASE WHEN EXISTS (SELECT 1 FROM __diff_renames WHERE id = "
		                                        "%llu) AND __diff_ro%llu IS DISTINCT FROM __diff_rn%llu THEN %s END",
		                                        r, r, r, L(new_side.names[new_index])));
	}
	for (idx_t p = 0; p < swap_candidates.size(); p++) {
		auto &cand = swap_candidates[p];
		// a crossed identity is displayed under the new name it ends at
		entries.emplace_back(common[cand.b].new_index,
		                     StringUtil::Format("CASE WHEN EXISTS (SELECT 1 FROM __diff_swaps WHERE id = %llu) AND "
		                                        "__diff_xo%llu IS DISTINCT FROM __diff_xn%llu THEN %s END",
		                                        p, p, p, L(common[cand.b].new_name)));
		entries.emplace_back(common[cand.a].new_index,
		                     StringUtil::Format("CASE WHEN EXISTS (SELECT 1 FROM __diff_swaps WHERE id = %llu) AND "
		                                        "__diff_yo%llu IS DISTINCT FROM __diff_yn%llu THEN %s END",
		                                        p, p, p, L(common[cand.a].new_name)));
	}
	std::stable_sort(
	    entries.begin(), entries.end(),
	    [](const std::pair<idx_t, string> &a, const std::pair<idx_t, string> &b) { return a.first < b.first; });
	vector<string> exprs;
	for (auto &entry : entries) {
		exprs.push_back(entry.second);
	}
	if (exprs.empty()) {
		exprs.push_back("NULL::VARCHAR");
	}
	sql += StringUtil::Join(exprs, ", ");
	sql += "]::VARCHAR[], lambda x: x IS NOT NULL) AS __diff_changed FROM __diff_m),\n";
}

void DiffSQLGenerator::GenerateSummaryCTEs(string &sql) {
	// R matched rows and C identified columns: a row edit costs log2(R) + log2(C choose c) bits, a column edit
	// log2(C) + log2(R choose k); a column is reported as a column edit when that is cheaper than its share of
	// the row edits that would otherwise cover its cells
	// the value columns already leave out a declared key; a guessed key is subtracted at runtime
	idx_t identified = value_columns.size();
	string key_width = declared_key.empty() ? "coalesce((SELECT width FROM __diff_key), 0)" : "0";
	sql += StringUtil::Format(
	    "__diff_dims AS (SELECT greatest((SELECT count(*) FROM __diff_m), 1) AS R, greatest(%llu - %s + "
	    "(SELECT count(*) FROM __diff_renames), 1) AS C),\n",
	    identified, key_width);
	sql +=
	    "__diff_rowcost AS (SELECT __diff_opos, __diff_changed, len(__diff_changed) AS cr, round(log2(R) + (lgamma(C + "
	    "1) - "
	    "lgamma(len(__diff_changed) + 1) - lgamma(C - len(__diff_changed) + 1)) / ln(2), 2) / len(__diff_changed) AS "
	    "share FROM __diff_cells, __diff_dims WHERE len(__diff_changed) > 0),\n";
	sql += "__diff_cellrows AS (SELECT __diff_opos, cr, share, unnest(__diff_changed) AS col FROM __diff_rowcost),\n";
	sql +=
	    "__diff_coldec AS (SELECT col, count(*) AS changes, list(__diff_opos ORDER BY __diff_opos) AS rows, sum(share) "
	    "AS cost_rows, round(log2(any_value(C)) + (lgamma(any_value(R) + 1) - lgamma(count(*) + 1) - "
	    "lgamma(any_value(R) - count(*) + 1)) / ln(2), 2) AS cost_col FROM __diff_cellrows, __diff_dims GROUP BY "
	    "col),\n";
	sql += "__diff_coledits AS (SELECT * FROM __diff_coldec WHERE cost_col < cost_rows),\n";
	sql += "__diff_residual AS (SELECT __diff_opos FROM __diff_cellrows WHERE col NOT IN (SELECT col FROM "
	       "__diff_coledits) GROUP BY __diff_opos),\n";
	sql += "__diff_rowedits AS (SELECT r.__diff_changed AS cols, count(*) AS rows, sum(r.cr)::BIGINT AS changes, "
	       "min(r.__diff_opos) AS first_row FROM __diff_rowcost r JOIN __diff_residual USING (__diff_opos) GROUP BY "
	       "r.__diff_changed),\n";
	// columns that changed in exactly the same rows are one line; order them as the new relation does
	sql += "__diff_colpos AS (SELECT * FROM (VALUES ";
	vector<string> positions;
	for (idx_t i = 0; i < new_side.names.size(); i++) {
		positions.push_back(StringUtil::Format("(%s, %llu)", L(new_side.names[i]), i));
	}
	sql += StringUtil::Join(positions, ", ") + ") t(col, pos)),\n";
	sql += "__diff_coleditgroups AS (SELECT list(e.col ORDER BY p.pos) AS cols, len(any_value(e.rows)) AS rows, "
	       "sum(e.changes)::BIGINT AS changes, min(p.pos) AS first_col FROM __diff_coledits e JOIN __diff_colpos p ON "
	       "p.col = e.col GROUP BY e.rows),\n";
	// the diff is implausible - the relation was regenerated rather than edited - when more than half of all cells
	// changed under a key the tool chose itself
	sql += StringUtil::Format(
	    "__diff_mass AS (SELECT ((SELECT n FROM __diff_dropped) * %llu + (CASE WHEN (SELECT basis FROM __diff_key) IS "
	    "NULL THEN 0 ELSE (SELECT n FROM __diff_added) END) * %llu + 2 * (SELECT coalesce(sum(cr), 0) FROM "
	    "__diff_rowcost))::HUGEINT AS changed, ((SELECT count(*) FROM __diff_o) * %llu + (SELECT count(*) FROM "
	    "__diff_n) "
	    "* %llu)::HUGEINT AS total),\n",
	    old_side.names.size(), new_side.names.size(), old_side.names.size(), new_side.names.size());
	sql += "__diff_regen AS (SELECT " + string(declared_key.empty() ? "changed * 2 > total" : "false") +
	       " AS regenerate FROM __diff_mass)\n";
}

//! The minimum set of columns that must move to explain the new column order, as (old position, new position)
static vector<std::pair<idx_t, idx_t>> ColumnMoves(const vector<DiffColumn> &common) {
	// the longest increasing subsequence of new positions (in old order) stays; every other column moved
	idx_t n = common.size();
	vector<idx_t> length(n, 1);
	vector<optional_idx> prev(n);
	for (idx_t i = 0; i < n; i++) {
		for (idx_t j = 0; j < i; j++) {
			if (common[j].new_index < common[i].new_index && length[j] + 1 > length[i]) {
				length[i] = length[j] + 1;
				prev[i] = j;
			}
		}
	}
	vector<bool> stays(n, false);
	if (n > 0) {
		idx_t best = 0;
		for (idx_t i = 1; i < n; i++) {
			if (length[i] > length[best]) {
				best = i;
			}
		}
		optional_idx cursor = best;
		while (cursor.IsValid()) {
			stays[cursor.GetIndex()] = true;
			cursor = prev[cursor.GetIndex()];
		}
	}
	vector<std::pair<idx_t, idx_t>> moves;
	for (idx_t i = 0; i < n; i++) {
		if (!stays[i]) {
			moves.emplace_back(common[i].old_index, common[i].new_index);
		}
	}
	return moves;
}

//! The type of the "change" column: one struct member per operation
static const char *DIFF_CHANGE_TYPE =
    "UNION(table_key STRUCT(columns VARCHAR[], basis VARCHAR, overlap DOUBLE), col_add STRUCT(\"column\" VARCHAR), "
    "col_drop STRUCT(\"column\" VARCHAR), col_rename STRUCT(old_column VARCHAR, new_column VARCHAR, basis VARCHAR), "
    "col_edit STRUCT(columns VARCHAR[], \"rows\" BIGINT, changes BIGINT, old_type VARCHAR, new_type VARCHAR), "
    "col_order STRUCT(\"column\" VARCHAR, old_position BIGINT, new_position BIGINT), row_add STRUCT(\"rows\" BIGINT), "
    "row_drop STRUCT(\"rows\" BIGINT), row_edit STRUCT(columns VARCHAR[], \"rows\" BIGINT, changes BIGINT), "
    "row_fanout STRUCT(\"rows\" BIGINT, new_rows BIGINT), table_regenerate STRUCT(changed HUGEINT, total HUGEINT))";

//! A SQL expression spelling a name as data-diff does: a bare word when it is one, JSON-quoted otherwise
static string DisplayNameExpr(const string &expr) {
	return "CASE WHEN " + expr + " = ':row' OR regexp_matches(" + expr + ", '^[A-Za-z_][A-Za-z0-9_]*$') THEN " + expr +
	       " ELSE '\"' || replace(replace(" + expr + ", '\\', '\\\\'), '\"', '\\\"') || '\"' END";
}

//! A SQL expression rendering a list of names as "[a, b]"
static string DisplayListExpr(const string &expr) {
	return "'[' || list_aggr(list_transform(" + expr + ", lambda x: " + DisplayNameExpr("x") +
	       "), 'string_agg', ', ') || ']'";
}

string DiffSQLGenerator::GenerateSummaryResult() {
	// every line of the summary: (ord, sub, change), ordered schema first, then the row story
	vector<string> parts;
	string key_cols =
	    declared_key.empty() ? "coalesce((SELECT cols FROM __diff_key), [':row'])" : "(SELECT cols FROM __diff_key)";
	parts.push_back("SELECT 0 AS ord, 0 AS sub, union_value(table_key := struct_pack(columns := " + key_cols +
	                ", basis := coalesce((SELECT basis FROM __diff_key), 'fallback'), overlap := (SELECT overlap FROM "
	                "__diff_key)))::" +
	                DIFF_CHANGE_TYPE + " AS change");
	// the schema: drops and adds that are not renames, renames and exchanges, type changes, moves
	for (auto d : dropped) {
		parts.push_back(StringUtil::Format("SELECT 10, %llu, union_value(col_drop := struct_pack(\"column\" := %s)) "
		                                   "WHERE NOT EXISTS (SELECT 1 FROM __diff_renames WHERE old_col = %s)",
		                                   d, L(old_side.names[d]), L(old_side.names[d])));
	}
	for (auto a : added) {
		parts.push_back(StringUtil::Format("SELECT 11, %llu, union_value(col_add := struct_pack(\"column\" := %s)) "
		                                   "WHERE NOT EXISTS (SELECT 1 FROM __diff_renames WHERE new_col = %s)",
		                                   a, L(new_side.names[a]), L(new_side.names[a])));
	}
	parts.push_back("SELECT 12, id, union_value(col_rename := struct_pack(old_column := old_col, new_column := "
	                "new_col, basis := basis)) FROM __diff_renames");
	parts.push_back("SELECT 12, 1000 + 2 * id, union_value(col_rename := struct_pack(old_column := a, new_column := "
	                "b, basis := 'swapped')) FROM __diff_swaps");
	parts.push_back("SELECT 12, 1001 + 2 * id, union_value(col_rename := struct_pack(old_column := b, new_column := "
	                "a, basis := 'swapped')) FROM __diff_swaps");
	for (idx_t c = 0; c < common.size(); c++) {
		auto &col = common[c];
		if (col.old_type != col.new_type) {
			// an exchanged column keeps its own type
			auto name = L(col.new_name);
			parts.push_back(StringUtil::Format(
			    "SELECT 13, %llu, union_value(col_edit := struct_pack(columns := [%s]::VARCHAR[], \"rows\" := "
			    "NULL::BIGINT, changes := NULL::BIGINT, old_type := %s, new_type := %s)) WHERE NOT EXISTS (SELECT 1 "
			    "FROM "
			    "__diff_swaps WHERE a = %s OR b = %s)",
			    c, name, L(col.old_type.ToString()), L(col.new_type.ToString()), name, name));
		}
	}
	idx_t move_idx = 0;
	for (auto &move : ColumnMoves(common)) {
		parts.push_back(StringUtil::Format("SELECT 14, %llu, union_value(col_order := struct_pack(\"column\" := %s, "
		                                   "old_position := %llu::BIGINT, new_position := %llu::BIGINT))",
		                                   move_idx++, L(new_side.names[move.second]), move.first + 1,
		                                   move.second + 1));
	}
	parts.push_back("SELECT 14, 1000 + id, union_value(col_order := struct_pack(\"column\" := a, old_position := "
	                "old_position::BIGINT, new_position := new_position::BIGINT)) FROM __diff_swaps");
	// the rows and values - withheld when the relation was regenerated
	string live = " WHERE NOT (SELECT regenerate FROM __diff_regen)";
	parts.push_back("SELECT 20, first_col, union_value(col_edit := struct_pack(columns := cols, \"rows\" := rows, "
	                "changes := changes, old_type := NULL::VARCHAR, new_type := NULL::VARCHAR)) FROM "
	                "__diff_coleditgroups" +
	                live);
	parts.push_back("SELECT 21, 0, union_value(row_drop := struct_pack(\"rows\" := n)) FROM __diff_dropped WHERE n > "
	                "0 AND NOT (SELECT regenerate FROM __diff_regen)");
	parts.push_back("SELECT 22, 0, union_value(row_add := struct_pack(\"rows\" := n)) FROM __diff_added WHERE n > 0 "
	                "AND NOT (SELECT regenerate FROM __diff_regen)");
	parts.push_back("SELECT 23, first_row, union_value(row_edit := struct_pack(columns := cols, \"rows\" := rows, "
	                "changes := changes)) FROM __diff_rowedits" +
	                live);
	parts.push_back("SELECT 24, 0, union_value(row_fanout := struct_pack(\"rows\" := groups, new_rows := new_rows)) "
	                "FROM __diff_fan WHERE groups > 0 AND NOT (SELECT regenerate FROM __diff_regen)");
	parts.push_back("SELECT 30, 0, union_value(table_regenerate := struct_pack(changed := changed, total := total)) "
	                "FROM __diff_mass WHERE (SELECT regenerate FROM __diff_regen)");

	// the summary line in data-diff's grammar: kind(claim, field: value, ...)
	string summary = "CASE op";
	summary += " WHEN 'table_key' THEN 'table_key(' || " + DisplayListExpr("change.table_key.columns") +
	           " || ', basis: ' || change.table_key.basis || coalesce(', overlap: ' || format('{:.2f}', "
	           "change.table_key.overlap), '') || ')'";
	summary += " WHEN 'col_add' THEN 'col_add(' || " + DisplayNameExpr("change.col_add.\"column\"") + " || ')'";
	summary += " WHEN 'col_drop' THEN 'col_drop(' || " + DisplayNameExpr("change.col_drop.\"column\"") + " || ')'";
	summary += " WHEN 'col_rename' THEN 'col_rename(' || " + DisplayNameExpr("change.col_rename.old_column") +
	           " || ' -> ' || " + DisplayNameExpr("change.col_rename.new_column") +
	           " || ', basis: ' || change.col_rename.basis || ')'";
	summary += " WHEN 'col_edit' THEN 'col_edit(' || list_aggr(list_transform(change.col_edit.columns, lambda x: " +
	           DisplayNameExpr("x") +
	           "), 'string_agg', ', ') || CASE WHEN change.col_edit.old_type IS NOT NULL THEN ', type: ' || "
	           "change.col_edit.old_type || ' -> ' || change.col_edit.new_type ELSE CASE WHEN "
	           "len(change.col_edit.columns) > 1 THEN ', rows: ' || change.col_edit.\"rows\" ELSE '' END || ', "
	           "changes: ' || change.col_edit.changes END || ')'";
	summary += " WHEN 'col_order' THEN 'col_order(' || " + DisplayNameExpr("change.col_order.\"column\"") +
	           " || ', ' || change.col_order.old_position || ' -> ' || change.col_order.new_position || ')'";
	summary += " WHEN 'row_add' THEN 'row_add(rows: ' || change.row_add.\"rows\" || ')'";
	summary += " WHEN 'row_drop' THEN 'row_drop(rows: ' || change.row_drop.\"rows\" || ')'";
	summary += " WHEN 'row_edit' THEN 'row_edit(rows: ' || change.row_edit.\"rows\" || ', changes: ' || "
	           "change.row_edit.changes || CASE WHEN len(change.row_edit.columns) <= 5 THEN ', columns: ' || " +
	           DisplayListExpr("change.row_edit.columns") + " ELSE '' END || ')'";
	summary += " WHEN 'row_fanout' THEN 'row_fanout(rows: ' || change.row_fanout.\"rows\" || ')'";
	summary += " WHEN 'table_regenerate' THEN 'table_regenerate()' END";

	string sql = "SELECT op, change, " + summary +
	             " AS summary FROM (SELECT ord, sub, union_tag(change)::VARCHAR AS op, change FROM (\n";
	sql += StringUtil::Join(parts, "\nUNION ALL\n");
	sql += "\n) __diff_lines(ord, sub, change)) __diff_result ORDER BY ord, sub";
	return sql;
}

string DiffSQLGenerator::GenerateCellsResult() {
	// one row per changed cell of a matched row, and one per added or dropped row
	vector<string> entries;
	for (auto c : value_columns) {
		auto name = L(common[c].new_name);
		string in_key = InKey(c) + " OR EXISTS (SELECT 1 FROM __diff_swaps WHERE a = " + name + " OR b = " + name + ")";
		entries.push_back(
		    StringUtil::Format("CASE WHEN NOT (%s) AND __diff_o%llu IS DISTINCT FROM __diff_n%llu THEN "
		                       "struct_pack(col := %s, old_value := CAST(__diff_o%llu AS VARCHAR), new_value := "
		                       "CAST(__diff_n%llu AS VARCHAR)) END",
		                       in_key, c, c, name, c, c));
	}
	for (idx_t r = 0; r < rename_candidates.size(); r++) {
		entries.push_back(StringUtil::Format(
		    "CASE WHEN EXISTS (SELECT 1 FROM __diff_renames WHERE id = %llu) AND __diff_ro%llu IS DISTINCT FROM "
		    "__diff_rn%llu THEN struct_pack(col := %s, old_value := CAST(__diff_ro%llu AS VARCHAR), new_value := "
		    "CAST(__diff_rn%llu AS VARCHAR)) END",
		    r, r, r, L(new_side.names[rename_candidates[r].new_index]), r, r));
	}
	for (idx_t p = 0; p < swap_candidates.size(); p++) {
		auto &cand = swap_candidates[p];
		entries.push_back(StringUtil::Format(
		    "CASE WHEN EXISTS (SELECT 1 FROM __diff_swaps WHERE id = %llu) AND __diff_xo%llu IS DISTINCT FROM "
		    "__diff_xn%llu THEN struct_pack(col := %s, old_value := CAST(__diff_xo%llu AS VARCHAR), new_value := "
		    "CAST(__diff_xn%llu AS VARCHAR)) END",
		    p, p, p, L(common[cand.b].new_name), p, p));
		entries.push_back(StringUtil::Format(
		    "CASE WHEN EXISTS (SELECT 1 FROM __diff_swaps WHERE id = %llu) AND __diff_yo%llu IS DISTINCT FROM "
		    "__diff_yn%llu THEN struct_pack(col := %s, old_value := CAST(__diff_yo%llu AS VARCHAR), new_value := "
		    "CAST(__diff_yn%llu AS VARCHAR)) END",
		    p, p, p, L(common[cand.a].new_name), p, p));
	}
	if (entries.empty()) {
		entries.push_back("NULL::STRUCT(col VARCHAR, old_value VARCHAR, new_value VARCHAR)");
	}
	string sql = "SELECT op, key, \"column\", old_value, new_value FROM (\n";
	sql += "SELECT 'row_edit' AS op, __diff_k AS key, __diff_opos AS ord, cell.col AS \"column\", cell.old_value, "
	       "cell.new_value FROM (SELECT __diff_k, __diff_opos, unnest(list_filter([" +
	       StringUtil::Join(entries, ", ") + "], lambda x: x IS NOT NULL)) AS cell FROM __diff_m)\n";
	sql += "UNION ALL SELECT 'row_drop', o.__diff_k, o.__diff_pos, NULL, NULL, NULL FROM __diff_ok o WHERE NOT EXISTS "
	       "(SELECT 1 FROM __diff_nk n WHERE n.__diff_k = o.__diff_k)\n";
	sql += "UNION ALL SELECT 'row_add', n.__diff_k, n.__diff_pos, NULL, NULL, NULL FROM __diff_nk n WHERE NOT EXISTS "
	       "(SELECT 1 FROM __diff_ok o WHERE o.__diff_k = n.__diff_k)\n";
	sql += ") __diff_result ORDER BY op, ord, \"column\"";
	return sql;
}

string DiffSQLGenerator::Generate() {
	AnalyzeSchema();
	ResolveDeclaredKey();
	EnumerateKeyCandidates();
	for (idx_t c = 0; c < common.size(); c++) {
		if (!common[c].comparable) {
			continue;
		}
		bool in_declared_key = false;
		for (auto k : declared_key) {
			in_declared_key = in_declared_key || k == c;
		}
		if (!in_declared_key) {
			value_columns.push_back(c);
		}
	}
	EnumerateSwapCandidates();
	string sql;
	GenerateSides(sql);
	GenerateKey(sql);
	GenerateMatching(sql);
	GenerateRenames(sql);
	GenerateCells(sql);
	GenerateSummaryCTEs(sql);
	sql += ref.cells ? GenerateCellsResult() : GenerateSummaryResult();
	return sql;
}

} // namespace

//! Bind one side once to learn its columns, and render it as the SQL the generated query embeds
static DiffSide BindDiffSide(Binder &binder, ClientContext &context, TableRef &side) {
	DiffSide result;
	auto copy = side.Copy();
	auto child_binder = Binder::CreateBinder(context, &binder);
	auto bound = child_binder->Bind(*copy);
	for (auto &name : bound.names) {
		result.names.push_back(name.GetIdentifierName());
	}
	result.types = bound.types;
	if (side.type == TableReferenceType::SUBQUERY) {
		result.sql = side.Cast<SubqueryRef>().subquery->ToString();
	} else {
		result.sql = "SELECT * FROM " + side.ToString();
	}
	return result;
}

BoundStatement Binder::Bind(DiffRef &ref) {
	auto old_side = BindDiffSide(*this, context, *ref.old_side);
	auto new_side = BindDiffSide(*this, context, *ref.new_side);
	DiffSQLGenerator generator(context, ref, std::move(old_side), std::move(new_side));
	auto sql = generator.Generate();
	auto select = CreateViewInfo::ParseSelect(sql);
	auto subquery = make_uniq<SubqueryRef>(std::move(select), "diff");
	return Bind(*subquery);
}

} // namespace duckdb
