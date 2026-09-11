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

TEST_CASE("TemporaryMemoryManager exposes memory-limit-aware reservations", "[storage][temporary_memory_manager]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();

	constexpr idx_t MIB = 1024ULL * 1024ULL;
	constexpr idx_t memory_limit = 100ULL * MIB;
	constexpr idx_t large_minimum_reservation = 80ULL * MIB;
	constexpr idx_t small_minimum_reservation = 40ULL * MIB;

	buffer_pool.SetLimit(memory_limit, "temporary memory manager limit-aware reservation test");
	REQUIRE_NO_FAIL(con.Query("SET temp_directory='" + TestCreatePath("limit_aware_reservation_temp") + "'"));

	auto &manager = TemporaryMemoryManager::Get(context);
	auto large_state = manager.Register(context);
	large_state->SetMinimumReservation(large_minimum_reservation);
	large_state->SetRemainingSizeAndUpdateReservation(context, large_minimum_reservation);
	REQUIRE(large_state->GetReservationForMemoryLimit() == large_state->GetReservation());

	auto small_state = manager.Register(context);
	small_state->SetMinimumReservation(small_minimum_reservation);
	small_state->SetRemainingSizeAndUpdateReservation(context, small_minimum_reservation);

	REQUIRE(large_state->GetReservation() + small_state->GetReservation() > memory_limit);
	REQUIRE(large_state->GetReservationForMemoryLimit() < large_state->GetReservation());
	REQUIRE(small_state->GetReservationForMemoryLimit() < small_state->GetReservation());
}
