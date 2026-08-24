#include "catch.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("TemporaryMemoryManager handles many active states", "[storage][temporary_memory_manager]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	constexpr idx_t memory_limit = 1024ULL * 1024ULL * 1024ULL;
	constexpr idx_t state_count = 80;
	constexpr idx_t initial_reservation = 10ULL * 1024ULL * 1024ULL;
	constexpr idx_t remaining_size = 1024ULL * 1024ULL * 1024ULL;

	buffer_pool.SetLimit(memory_limit, "temporary memory manager test");

	auto &manager = TemporaryMemoryManager::Get(context);
	duckdb::vector<duckdb::unique_ptr<TemporaryMemoryState>> states;
	states.reserve(state_count);
	for (idx_t i = 0; i < state_count; i++) {
		auto state = manager.Register(context);
		state->SetMinimumReservation(initial_reservation);
		state->SetRemainingSize(remaining_size);
		states.push_back(std::move(state));
	}

	REQUIRE_NO_FAIL(con.Query("SET debug_force_external=true"));
	for (auto &state : states) {
		state->UpdateReservation(context);
		REQUIRE(state->GetReservation() == initial_reservation);
	}

	REQUIRE_NO_FAIL(con.Query("SET debug_force_external=false"));
	states.back()->UpdateReservation(context);

	REQUIRE(states.back()->GetReservation() >= initial_reservation);
	REQUIRE(states.back()->GetReservation() <= remaining_size);
}
