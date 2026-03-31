#include "duckdb/execution/operator/helper/physical_fan_out.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/interrupt.hpp"

namespace duckdb {

PhysicalFanOut::PhysicalFanOut(PhysicalPlan &plan, PhysicalOperator &child_source_p, idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::FAN_OUT, child_source_p.types, estimated_cardinality),
      child_source(child_source_p) {
}

//===--------------------------------------------------------------------===//
// State
//===--------------------------------------------------------------------===//

static constexpr idx_t BATCH_SIZE = 128;
static constexpr idx_t NUM_BUFFERS = 8;
static constexpr idx_t CHUNKS_PER_BATCH = 8;

struct ChunkBuffer {
	DataChunk chunks[BATCH_SIZE];
	idx_t batch_indices[BATCH_SIZE];
	idx_t count = 0;
	atomic<idx_t> next_claim {0};
	atomic<idx_t> completed {0};
	atomic<idx_t> accessing {0};

	void Initialize(const vector<LogicalType> &types) {
		for (idx_t i = 0; i < BATCH_SIZE; i++) {
			chunks[i].Initialize(Allocator::DefaultAllocator(), types);
		}
	}

	//! Returns true if all slots have been consumed and no thread is mid-access
	bool IsFullyCompleted() const {
		return count > 0 && completed.load(std::memory_order_acquire) >= count &&
		       accessing.load(std::memory_order_acquire) == 0;
	}

	void PrepareForFill(const vector<LogicalType> &types) {
		for (idx_t i = 0; i < count; i++) {
			if (chunks[i].ColumnCount() == 0) {
				chunks[i].Initialize(Allocator::DefaultAllocator(), types);
			}
		}
		count = 0;
		next_claim.store(0, std::memory_order_relaxed);
		completed.store(0, std::memory_order_relaxed);
	}
};

class FanOutGlobalSourceState : public GlobalSourceState {
public:
	FanOutGlobalSourceState(ClientContext &context, const PhysicalFanOut &op)
	    : child_types(op.child_source.get().types) {
		child_global = op.child_source.get().GetGlobalSourceState(context);
		for (idx_t i = 0; i < NUM_BUFFERS; i++) {
			buffers[i].Initialize(child_types);
		}
	}

	unique_ptr<GlobalSourceState> child_global;
	unique_ptr<LocalSourceState> child_local;
	bool child_local_initialized = false;
	mutex init_lock;
	const vector<LogicalType> &child_types;

	//! N buffers — producer fills in order, consumers read in order
	ChunkBuffer buffers[NUM_BUFFERS];
	atomic<idx_t> consume_idx {0};

	//! Producer
	atomic<bool> producing {false};
	atomic<bool> exhausted {false};
	idx_t next_batch = 0;
	atomic<idx_t> produce_idx {0};

	//! Blocked consumers waiting for data
	mutex blocked_lock;
	vector<InterruptState> blocked_consumers;

	//! Temp chunk for Produce
	DataChunk temp_chunk;
	bool temp_chunk_initialized = false;

	idx_t MaxThreads() override {
		return NumericLimits<idx_t>::Maximum();
	}
};

class FanOutLocalSourceState : public LocalSourceState {
public:
	idx_t current_batch = 0;

	//! Stashed chunks from block-based consumption
	struct StashedChunk {
		unique_ptr<DataChunk> chunk;
		idx_t batch_index;
	};
	vector<StashedChunk> stash;
	idx_t stash_idx = 0;
};

//===--------------------------------------------------------------------===//
// Init
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSourceState> PhysicalFanOut::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq_base<GlobalSourceState, FanOutGlobalSourceState>(context, *this);
}

unique_ptr<LocalSourceState> PhysicalFanOut::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate) const {
	auto &fan_gstate = gstate.Cast<FanOutGlobalSourceState>();
	auto result = make_uniq<FanOutLocalSourceState>();
	// Single shared child local state — only one thread produces
	lock_guard<mutex> lock(fan_gstate.init_lock);
	if (!fan_gstate.child_local_initialized) {
		fan_gstate.child_local = child_source.get().GetLocalSourceState(context, *fan_gstate.child_global);
		fan_gstate.child_local_initialized = true;
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// TryConsume — claim a slot from the current consume buffer
//===--------------------------------------------------------------------===//
static bool TryConsume(FanOutGlobalSourceState &gstate, FanOutLocalSourceState &lstate, DataChunk &chunk) {
	// First check stash from a previous block claim
	if (lstate.stash_idx < lstate.stash.size()) {
		auto &entry = lstate.stash[lstate.stash_idx];
		chunk.Append(*entry.chunk);
		lstate.current_batch = entry.batch_index;
		lstate.stash_idx++;
		if (lstate.stash_idx >= lstate.stash.size()) {
			lstate.stash.clear();
			lstate.stash_idx = 0;
		}
		return true;
	}

	idx_t cidx = gstate.consume_idx.load(std::memory_order_acquire);
	idx_t pidx = gstate.produce_idx.load(std::memory_order_acquire);
	if (cidx >= pidx) {
		return false; // no filled buffers
	}
	auto &buf = gstate.buffers[cidx % NUM_BUFFERS];

	// Register intent to access this buffer before checking anything
	buf.accessing.fetch_add(1, std::memory_order_acq_rel);

	// Verify consume_idx hasn't moved — if it has, the buffer may be recycled
	if (gstate.consume_idx.load(std::memory_order_acquire) != cidx) {
		buf.accessing.fetch_sub(1, std::memory_order_release);
		return false;
	}

	// Claim a whole block of CHUNKS_PER_BATCH slots
	idx_t my_slot = buf.next_claim.fetch_add(CHUNKS_PER_BATCH, std::memory_order_acq_rel);
	if (my_slot >= buf.count) {
		buf.accessing.fetch_sub(1, std::memory_order_release);
		return false;
	}

	// How many slots we actually got (may be less at end of buffer)
	idx_t slots_claimed = MinValue<idx_t>(CHUNKS_PER_BATCH, buf.count - my_slot);
	D_ASSERT(slots_claimed > 0);
	D_ASSERT(my_slot + slots_claimed <= BATCH_SIZE);

	// First chunk goes directly into output
	chunk.Append(buf.chunks[my_slot]);
	buf.chunks[my_slot].Reset();
	lstate.current_batch = buf.batch_indices[my_slot];

	// Remaining chunks go into the stash
	lstate.stash.clear();
	for (idx_t i = 1; i < slots_claimed; i++) {
		idx_t slot = my_slot + i;
		auto stash_chunk = make_uniq<DataChunk>();
		stash_chunk->Initialize(Allocator::DefaultAllocator(), buf.chunks[slot].GetTypes());
		stash_chunk->Append(buf.chunks[slot]);
		buf.chunks[slot].Reset();
		lstate.stash.push_back({std::move(stash_chunk), buf.batch_indices[slot]});
	}
	lstate.stash_idx = 0;

	idx_t done = buf.completed.fetch_add(slots_claimed, std::memory_order_release) + slots_claimed;

	// Last consumer advances consume_idx BEFORE releasing accessing.
	if (done >= buf.count) {
		gstate.consume_idx.fetch_add(1, std::memory_order_release);
	}

	// Release our access — producer can now recycle if accessing reaches 0
	buf.accessing.fetch_sub(1, std::memory_order_release);
	return true;
}

//===--------------------------------------------------------------------===//
// Produce — fill buffers from the child source
//===--------------------------------------------------------------------===//
static void Produce(FanOutGlobalSourceState &gstate, const PhysicalFanOut &op, ExecutionContext &context,
                    InterruptState &interrupt_state) {
	while (true) {
		// Check if we've been signaled to stop
		if (gstate.exhausted.load(std::memory_order_acquire)) {
			break;
		}
		// Can we fill? Check that the target buffer is fully completed
		auto pidx = gstate.produce_idx.load(std::memory_order_relaxed);
		auto &fill_buf = gstate.buffers[pidx % NUM_BUFFERS];
		// For the first fill (count==0) the buffer is unused. For subsequent fills,
		// wait until all consumers have completed reading from it.
		if (fill_buf.count > 0 && !fill_buf.IsFullyCompleted()) {
			break; // buffer still in use by consumers
		}
		fill_buf.PrepareForFill(gstate.child_types);

		// Initialize temp chunk on first use
		if (!gstate.temp_chunk_initialized) {
			gstate.temp_chunk.Initialize(Allocator::DefaultAllocator(), gstate.child_types);
			gstate.temp_chunk_initialized = true;
		}

		// Fill at full speed — no atomics in this loop
		bool done = false;
		while (fill_buf.count < BATCH_SIZE && !done) {
			gstate.temp_chunk.Reset();

			OperatorSourceInput child_input {*gstate.child_global, *gstate.child_local, interrupt_state};
			auto result = op.child_source.get().GetData(context, gstate.temp_chunk, child_input);

			// Deep copy into buffer so source can reuse its internal buffers
			if (gstate.temp_chunk.size() > 0) {
				D_ASSERT(fill_buf.count < BATCH_SIZE);
				auto &dest = fill_buf.chunks[fill_buf.count];
				dest.Reset();
				dest.Append(gstate.temp_chunk);
				gstate.temp_chunk.Reset();
				fill_buf.batch_indices[fill_buf.count] = gstate.next_batch / CHUNKS_PER_BATCH;
				gstate.next_batch++;
				fill_buf.count++;
			}

			// Handle return code
			switch (result) {
			case SourceResultType::FINISHED:
				gstate.exhausted.store(true, std::memory_order_release);
				done = true;
				break;
			case SourceResultType::BLOCKED:
				done = true;
				break;
			case SourceResultType::HAVE_MORE_OUTPUT:
				break;
			}
		}

		if (fill_buf.count > 0) {
			// Publish — single store makes buffer visible to consumers
			idx_t new_pidx = gstate.produce_idx.load(std::memory_order_relaxed) + 1;
			gstate.produce_idx.store(new_pidx, std::memory_order_release);

			// Wake up blocked consumers
			lock_guard<mutex> lock(gstate.blocked_lock);
			for (auto &blocked : gstate.blocked_consumers) {
				blocked.Callback();
			}
			gstate.blocked_consumers.clear();
		}

		if (gstate.exhausted.load(std::memory_order_relaxed)) {
			break;
		}
	}

	gstate.producing.store(false, std::memory_order_release);

	// Wake blocked consumers — producer done or out of buffer space
	{
		lock_guard<mutex> lock(gstate.blocked_lock);
		for (auto &blocked : gstate.blocked_consumers) {
			blocked.Callback();
		}
		gstate.blocked_consumers.clear();
	}
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType PhysicalFanOut::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<FanOutGlobalSourceState>();
	auto &lstate = input.local_state.Cast<FanOutLocalSourceState>();

	if (TryConsume(gstate, lstate, chunk)) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	if (gstate.exhausted.load(std::memory_order_acquire)) {
		if (TryConsume(gstate, lstate, chunk)) {
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		return SourceResultType::FINISHED;
	}

	bool expected = false;
	if (gstate.producing.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
		Produce(gstate, *this, context, input.interrupt_state);

		if (TryConsume(gstate, lstate, chunk)) {
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		return gstate.exhausted.load(std::memory_order_acquire) ? SourceResultType::FINISHED
		                                                        : SourceResultType::HAVE_MORE_OUTPUT;
	}

	// No data available — block until producer publishes
	{
		lock_guard<mutex> lock(gstate.blocked_lock);
		// Re-check after taking lock: producer may have finished and fired wake-ups
		// between our CAS attempt and acquiring the lock
		if (!gstate.producing.load(std::memory_order_acquire)) {
			// Producer is done — don't block, retry on next call
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		gstate.blocked_consumers.push_back(input.interrupt_state);
	}
	return SourceResultType::BLOCKED;
}

//===--------------------------------------------------------------------===//
// Partition data
//===--------------------------------------------------------------------===//
OperatorPartitionData PhysicalFanOut::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                       GlobalSourceState &gstate_p, LocalSourceState &lstate_p,
                                                       const OperatorPartitionInfo &partition_info) const {
	auto &fan_out_lstate = lstate_p.Cast<FanOutLocalSourceState>();
	OperatorPartitionData result(fan_out_lstate.current_batch);
	return result;
}

} // namespace duckdb
