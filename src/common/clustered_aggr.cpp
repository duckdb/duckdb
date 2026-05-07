#include "duckdb/common/clustered_aggr.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

// Why does clustering help performance so much in TPC-H q01? Yes, "in-register" sum (for example) is
// faster than load-add-store and avoids setting "bool is_set" every time - but this is the sub-plot only.
// The main reason is cache-consistency latency because multiple load-add-store patterns hit the
// same aggregate state in short succession. To illustrate that with TPC-H Q01, change SELECT
// l_returnflag,l_shipmode into SELECT CAST(l_quantity * 1.0 AS INT) .. GROUP BY ALL ORDER BY ALL.
// With the 1.0 constant there are 50 groups, but by decreasing it, one can scale flexibly down to
// fewer groups. Negative performance effects start around 25 and grow strongest at <= 5 groups.
//
// Is this only affecting aggregations with <25 groups? No, there are two more cases:
// - repeating keys: rolling up an ordered dataframe, or aggregating back to the PK of a FK-PK join.
//   e.g. change SELECT l_returnflag, l_shipmode into SELECT l_orderkey>>3, .. GROUP BY ALL
//   (orderkey repeats 1..7 timesavg 4 -- this is not enough, I put the threshold at 16)
// - (very) heavy hittng keys in a GROUP BY with potentially many groups
//   e.g. change SELECT l_returnflag, l_shipmode into SELECT (l_orderkey&1)*l_orderkey, .. GROUP BY ALL
//
// so we follow an adaptive strategy:
// - dual-cursor hash-lookup (for higher IPC) until 16 keys are seen ("hash_store")
// - after 1/32 of input switch to purely non-hash-based sequental run detection ("only_merge")
// - after seeing  16 keys, only hash-lookup those hot-keys, further only do run detection ("hash_merge")

// Slot bitfield widths (implementation detail)
constexpr int SLOT_GRP_BITS = 13;
constexpr int SLOT_CURSOR_BITS = 64 - 32 - SLOT_GRP_BITS; // 19 bits
static_assert(SLOT_GRP_BITS + SLOT_CURSOR_BITS + 32 == 64, "Slot bitfields must sum to 64");
static_assert((1 << SLOT_GRP_BITS) >= STANDARD_VECTOR_SIZE, "Group IDs must be able to address vectorsize");

struct Slot { // really just a 64-bits integer
	union {
		struct {
#if !defined(__BYTE_ORDER__) || __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
			uint64_t cursor : SLOT_CURSOR_BITS, idx : SLOT_GRP_BITS, gid : 32;
#else
			uint64_t gid : 32, idx : SLOT_GRP_BITS, cursor : SLOT_CURSOR_BITS;
#endif
		} bitfields;
		uint64_t i64; // allows to increment cursor without bit extraction logic
	} val;
	Slot(uint64_t c, uint64_t i, uint64_t g) {
		val.bitfields.cursor = c;
		val.bitfields.idx = i;
		val.bitfields.gid = g;
	}
};

struct SlotTab {
	Slot *tab;    // points into shared slots buffer (HASHTAB_SZ entries)
	sel_t *arena; // space for hashed group lists
	sel_t *seq;   // bump pointer for sequential (non-hashed) runs
};

static inline uint64_t slot_hash(uint64_t gid) {
	return ((gid * 17) ^ (gid >> ClusteredAggr::HASHTAB_LOG2)) & (ClusteredAggr::HASHTAB_SZ - 1);
}

bool ClusteredAggr::TryClustered(const uint64_t *group_ids, sel_t count, sel_t *arena, uint64_t *slots) {
	constexpr sel_t HALF_VEC = STANDARD_VECTOR_SIZE / 2;
	idx_t tuples_in_large = 0;
	idx_t hot_groups = 0;
	SlotTab st1, st2;
	st1.tab = reinterpret_cast<Slot *>(slots);
	st2.tab = reinterpret_cast<Slot *>(slots + HASHTAB_SZ);
	st1.arena = st2.arena = arena;
	st1.seq = arena + (MAX_HOT_KEYS + 1) * HALF_VEC;
	st2.seq = arena + (MAX_HOT_KEYS + 2) * HALF_VEC;

	auto finish_run = [&](SlotTab &st, const Slot s, idx_t threshold) -> uint64_t {
		const idx_t cnt = static_cast<idx_t>((st.arena + s.val.bitfields.cursor) - group_runs[s.val.bitfields.idx].sel);
		group_runs[s.val.bitfields.idx].count = cnt;
		if (s.val.bitfields.idx >= hot_groups) {
			st.seq += cnt; // sequential runs sequentially allocate from one buffer
		}
		if (cnt >= threshold) {
			tuples_in_large += cnt;
		}
		return s.val.bitfields.cursor;
	};

	auto free_slot = [&](SlotTab &st, uint64_t hash) {
		if (st.tab[hash].val.i64 != FREE_SLOT) {
			finish_run(st, st.tab[hash], HASH_RUNLENGTH_THRESHOLD);
			st.tab[hash].val.i64 = FREE_SLOT;
		}
	};

	auto new_run = [&](SlotTab &st, uint64_t gid, uint64_t list_start) -> Slot {
		uint64_t idx = n_group_runs++;
		group_runs[idx].sel = st.arena + list_start;
		group_runs[idx].gid = gid;
		return Slot(list_start, idx, gid);
	};

	auto hash_store = [&](SlotTab &st, uint64_t gid, sel_t pos) -> Slot {
		uint64_t hash = slot_hash(gid);
		Slot s = st.tab[hash];
		if (DUCKDB_UNLIKELY(s.val.i64 == FREE_SLOT)) { // hash slot free?
			s = new_run(st, gid, n_group_runs * HALF_VEC);
		} else if (DUCKDB_UNLIKELY(s.val.bitfields.gid != gid)) { // hash collision? overwrite!
			s = new_run(st, gid, finish_run(st, s, HASH_RUNLENGTH_THRESHOLD));
		}
		st.arena[s.val.bitfields.cursor] = pos; // append pos to run
		s.val.i64++; // this is really val.bitfields.cursor++ but without bit-extraction overhead
		return st.tab[hash] = s;
	};

	auto flush = [&](SlotTab &st, Slot s) {
		if (s.val.bitfields.idx < hot_groups) {
			st.tab[slot_hash(s.val.bitfields.gid)] = s; // store back hot slot in HT
		} else {
			finish_run(st, s, MERGE_RUNLENGTH_THRESHOLD); // finish non-hot (i.e. sequential) run
		}
	};

	auto hash_merge = [&](SlotTab &st, Slot cur, sel_t lo, sel_t hi) {
		for (sel_t pos = lo; pos < hi; pos++) {
			uint64_t gid = group_ids[pos];
			if (DUCKDB_UNLIKELY(cur.val.bitfields.gid != gid)) {
				flush(st, cur);
				Slot s = st.tab[slot_hash(gid)]; // do hash-lookup to detect hot gids
				cur = (s.val.i64 != FREE_SLOT && s.val.bitfields.gid == gid)
				          ? s
				          : new_run(st, gid, static_cast<uint64_t>(st.seq - st.arena));
			}
			st.arena[cur.val.bitfields.cursor] = pos; // append pos to run
			cur.val.i64++; // this is really val.bitfields.cursor++ but without bit-extraction overhead
		}
		if (lo < hi)
			flush(st, cur);
	};

	auto only_merge = [&](SlotTab &st, Slot cur, sel_t lo, sel_t hi) {
		for (sel_t pos = lo; pos < hi; pos++) {
			uint64_t gid = group_ids[pos];
			if (DUCKDB_UNLIKELY(cur.val.bitfields.gid != gid)) {
				flush(st, cur);
				cur = new_run(st, gid, static_cast<uint64_t>(st.seq - st.arena));
			}
			st.arena[cur.val.bitfields.cursor] = pos; // append pos to run
			cur.val.i64++; // this is really val.bitfields.cursor++ but without bit-extraction overhead
		}
		if (lo < hi)
			flush(st, cur);
	};

	auto free_table = [&](SlotTab &st) {
		for (sel_t i = 0; i < hot_groups; i++) {
			free_slot(st, slot_hash(group_runs[i].gid));
		}
	};

	// start with hash-based algorithm, using two cursors and two hashtables (for higher IPC)
	// ..but, first run an instrumented version that counts sequential hits on the first 3%
	Slot cur1(~0ULL, ~0ULL, ~0ULL);
	Slot cur2(~0ULL, ~0ULL, ~0ULL);
	idx_t miss1 = 0, miss2 = 0;
	sel_t pos;
	const sel_t half = count / 2, sample = count / 16;
	D_ASSERT(sample >= 32);
	for (pos = 0; pos < sample && n_group_runs < MAX_HOT_KEYS; pos++) {
		uint64_t gid1 = group_ids[pos];
		uint64_t gid2 = group_ids[pos + half];
		miss1 += (cur1.val.bitfields.gid != gid1);
		miss2 += (cur2.val.bitfields.gid != gid2);
		cur1 = hash_store(st1, gid1, pos);
		cur2 = hash_store(st2, gid2, pos + half);
	}
	if (miss1 + miss2 > n_group_runs + 2) {
		// continue with the hash-based strategy that works best when there are <16 groups
		for (; pos < half && n_group_runs < MAX_HOT_KEYS; pos++) {
			uint64_t gid1 = group_ids[pos];
			uint64_t gid2 = group_ids[pos + half];
			cur1 = hash_store(st1, gid1, pos);
			cur2 = hash_store(st2, gid2, pos + half);
		}
		hot_groups = n_group_runs; // hash table will be getting too full, switch strategy

		// still do hash-lookups (for hot-keys), but do not insert anymore; just detect runs
		hash_merge(st1, cur1, pos, half);
		hash_merge(st2, cur2, half + pos, count);
	} else {
		// almost fully clustered sequence!
		hot_groups = n_group_runs; // for free_table to work properly

		// forget the hash table, just look for sequential gids
		only_merge(st1, cur1, pos, half);
		only_merge(st2, cur2, half + pos, count);
	}
	free_table(st1);
	free_table(st2);
	cached_dict_sel = nullptr;
	bool ok = (2 * tuples_in_large >= count);
	return ok;
}

void ClusteredAggr::SetSingleRun(data_ptr_t state, idx_t count) {
	n_group_runs = 1;
	group_runs[0].state = state;
	group_runs[0].sel = nullptr;
	group_runs[0].gid = 0;
	group_runs[0].count = count;
	cached_dict_sel = nullptr;
}

void ClusteredAggr::AdvanceStates(idx_t payload_size) {
	for (idx_t r = 0; r < n_group_runs; r++) {
		group_runs[r].state += payload_size;
	}
}

const sel_t *ClusteredAggr::ClusterIter(const Vector &input, idx_t count) const {
	if (input.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return nullptr;
	}
	auto &child = DictionaryVector::Child(input);
	if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
		return nullptr;
	}
	auto &dict_sel = DictionaryVector::SelVector(input);
	const sel_t *dict_data = dict_sel.data();
	if (dict_data == nullptr) {
		return nullptr;
	}
	if (cached_dict_sel == dict_data) {
		return composed_sel_data;
	}
	idx_t pos = 0;
	for (idx_t r = 0; r < n_group_runs; r++) {
		const auto *run_sel = group_runs[r].sel;
		const auto run_count = group_runs[r].count;
		for (idx_t k = 0; k < run_count; k++) {
			auto idx = run_sel ? run_sel[k] : k;
			composed_sel_data[pos + k] = dict_data[idx];
		}
		pos += run_count;
	}
	cached_dict_sel = dict_data;
	return composed_sel_data;
}

void ClusteredAggrState::Initialize() {
	arena = make_unsafe_uniq_array_uninitialized<sel_t>((ClusteredAggr::MAX_HOT_KEYS + 3) * STANDARD_VECTOR_SIZE / 2);
	slots = make_unsafe_uniq_array_uninitialized<uint64_t>(2 * ClusteredAggr::HASHTAB_SZ);
	std::fill_n(slots.get(), 2 * ClusteredAggr::HASHTAB_SZ, ClusteredAggr::FREE_SLOT);
	skipped_opportunities = 0;
	retry_backoff = 1;
}

bool ClusteredAggrState::TryBuild(ClusteredAggr &clustered, const uint64_t *group_ids, idx_t count) {
	if (!arena) {
		return false;
	}
	if (skipped_opportunities > 0) {
		skipped_opportunities--;
		return false;
	}
	if (count >= 512 && clustered.TryClustered(group_ids, static_cast<sel_t>(count), arena.get(), slots.get())) {
		skipped_opportunities = 0;
		retry_backoff = 1;
		return true;
	}
	skipped_opportunities = retry_backoff;
	retry_backoff = MinValue<idx_t>(NumericLimits<idx_t>::Maximum() / 2, retry_backoff) * 2;
	return false;
}
} // namespace duckdb
