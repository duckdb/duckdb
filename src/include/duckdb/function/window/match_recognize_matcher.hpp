//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/match_recognize_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/match_recognize.hpp"
#include "duckdb/common/allocator.hpp"

#include <functional>

namespace duckdb {

class ClientContext;

//! An instruction of the compiled pattern. Compiling the tree into a program makes "what to do
//! after this node" a position in that program rather than a place in a recursive walk, which is
//! what lets the matcher recognise a state it has already explored.
enum class PatternOp : uint8_t { SYMBOL, SPLIT, JUMP, ANCHOR, MATCH };

struct PatternInstruction {
	PatternOp op = PatternOp::MATCH;
	//! SYMBOL: the variable to test, and whether it sits inside a {- -}
	idx_t symbol = 0;
	bool excluded = false;
	//! SPLIT: where to go first, then where to go if that fails. JUMP: where to go.
	idx_t target = 0;
	idx_t alternative = 0;
	//! ANCHOR: whether it holds past the partition's last row rather than at its first
	bool at_end = false;
};

//! Decides whether a row can be a given symbol. What that takes is the executor's business.
using SymbolMatcher = std::function<bool(idx_t symbol, idx_t row)>;

//! The pattern as a program the matcher walks
struct PatternProgram {
	//! A pattern whose program grows past this cannot be matched in any useful time anyway, and the
	//! memo the matcher keeps is one record per instruction per row
	static constexpr idx_t MAX_INSTRUCTIONS = 1 << 20;

	vector<PatternInstruction> code;

	//! `limit` bounds a counted quantifier: a repetition matching `n` rows can be reached at most
	//! `limit / n` times before the rows run out, so more repetitions than that are unreachable
	void Compile(const MatchRecognizePattern &node, idx_t limit, bool excluded = false);
	void Finish();

private:
	idx_t Emit(PatternOp op);
	void Push(const PatternInstruction &instruction);
};

//! How long a walked state stays proof that the search below it is a dead end. That depends on what
//! the conditions read, because a state is only a dead end for as long as its conditions answer the
//! same way.
enum class PatternMemo : uint8_t {
	//! Conditions read nothing but the row they test, so a dead end stays one for the whole partition
	PARTITION,
	//! A condition reads MATCH_NUMBER(), which is fixed within an attempt but differs between them
	ATTEMPT,
	//! A condition navigates the match, so a state's answer depends on the rows matched before it
	HISTORY
};

//! Walks the compiled program depth first, preferring the branch a greedy quantifier wants, and
//! stops at the first way through - which is the match the standard asks for.
//!
//! A (instruction, row) pair that has been explored once and did not lead to a match cannot lead to
//! one later, so it is never explored again. That is what keeps the search polynomial where plain
//! backtracking is exponential, and it holds only while the conditions answer the same way each time
//! that pair is reached. A condition that navigates the match being assembled reads the rows matched
//! before the one it tests, so two ways of reaching the same pair can disagree and the record has to
//! go.
//!
//! What is left there is cycle detection rather than memoisation: a walk may not reach the same
//! instruction twice without matching a row in between, because everything it could do the second
//! time it already did the first. That is a property of the path being walked, not of the search as
//! a whole, so the marks belong to the backtracking state. They are kept in an undo log: taking an
//! alternative back off the stack restores the marks to what they were on the path that reached it,
//! and matching a row opens a scope of its own that the marks left behind cannot answer for.
struct PatternMatcher {
	PatternMatcher(ClientContext &context, const PatternProgram &program, const SymbolMatcher &symbol_matches,
	               vector<idx_t> &classifiers, vector<uint8_t> &excluded_rows, PatternMemo memo);

	//! A partition is matched within its own bounds, and the anchors and row offsets a record was
	//! taken under only hold there, so nothing is carried over from the one before
	void BeginPartition();

	//! Match starting at `start`, within the partition [`partition_start`, `input_size`)
	bool Match(idx_t start, idx_t partition_start, idx_t input_size);

	//! One past the last row of the match, valid after Match() returned true
	idx_t match_end = 0;

private:
	//! A state still to be walked. Its scope names the stretch of the walk that reached it and
	//! matched no row, and `trail_size` the marks that stretch had taken by then.
	struct PendingState {
		idx_t pc;
		idx_t offset;
		idx_t scope;
		idx_t trail_size;
	};

	//! What a mark said before the walk overwrote it, so that backtracking can put it back
	struct HistoryMark {
		idx_t pc;
		idx_t scope;
	};

	//! Record that this state is being walked, or report that it already was
	bool Visit(idx_t pc, idx_t offset, idx_t walk);
	//! Retire every record taken so far
	void NextEpoch();
	//! Open the stretch of the walk that starts where the last row was matched
	idx_t NextScope();
	//! Put the marks back the way the path being resumed left them
	void UnwindHistory(idx_t trail_size);
	void ClearExplored();

	//! How many instructions the walk takes between two checks for a cancelled query
	static constexpr idx_t INTERRUPT_INTERVAL = 4096;

	ClientContext &context;
	const PatternProgram &program;
	const SymbolMatcher &symbol_matches;
	vector<idx_t> &classifiers;
	vector<uint8_t> &excluded_rows;
	PatternMemo memo;
	idx_t row_count;
	idx_t steps = 0;
	//! One record per (instruction, row): the epoch in which that state was walked
	AllocatedData explored;
	idx_t explored_size = 0;
	//! Records matching this belong to the current partition or attempt
	uint8_t epoch = 0;
	//! One mark per instruction, naming the scope the walk last visited it in. No row is matched
	//! within a scope, so every state it marks sits at the same row.
	vector<idx_t> history_marks;
	idx_t history_scope = 0;
	vector<HistoryMark> history_trail;
	//! The records this attempt wrote into the partition-wide memo, undone if it finds a match
	vector<idx_t> attempt_marks;
	vector<PendingState> pending;
};

} // namespace duckdb
