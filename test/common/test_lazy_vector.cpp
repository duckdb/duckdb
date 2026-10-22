#include "catch.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

namespace {
struct LazyAllocationState : PrivateAllocatorData {
	idx_t allocations = 0;
	idx_t live_bytes = 0;
	bool fail = false;

	static data_ptr_t Allocate(PrivateAllocatorData *data, idx_t size) {
		auto &state = data->Cast<LazyAllocationState>();
		if (state.fail) {
			throw OutOfMemoryException("lazy vector test allocation failure");
		}
		auto result = Allocator::DefaultAllocator().AllocateData(size);
		state.allocations++;
		state.live_bytes += size;
		return result;
	}
	static void Free(PrivateAllocatorData *data, data_ptr_t pointer, idx_t size) {
		data->Cast<LazyAllocationState>().live_bytes -= size;
		Allocator::DefaultAllocator().FreeData(pointer, size);
	}
	static data_ptr_t Reallocate(PrivateAllocatorData *data, data_ptr_t pointer, idx_t old_size, idx_t size) {
		auto result = Allocate(data, size);
		memcpy(result, pointer, MinValue(old_size, size));
		Free(data, pointer, old_size);
		return result;
	}
};
} // namespace

TEST_CASE("Chunk payload allocation is lazy by default", "[lazy_vector]") {
	auto state = make_uniq<LazyAllocationState>();
	auto &allocations = *state;
	Allocator allocator(LazyAllocationState::Allocate, LazyAllocationState::Free, LazyAllocationState::Reallocate,
	                    std::move(state));
	DataChunk chunk;
	chunk.Initialize(allocator, {LogicalType::BIGINT, LogicalType::VARCHAR}, 4096);
	REQUIRE(chunk.GetCapacity() == 4096);
	for (idx_t iteration = 0; iteration < 3; iteration++) {
		REQUIRE(chunk.size() == 0);
		REQUIRE(FlatVector::GetData(chunk.data[0]) == nullptr);
		REQUIRE(FlatVector::GetData(chunk.data[1]) == nullptr);
		UnifiedVectorFormat format;
		chunk.data[0].ToUnifiedFormat(format);
		REQUIRE(chunk.data[0].Values<int64_t>().size() == 0);
		chunk.Reset();
	}
	REQUIRE(allocations.allocations == 0);
	{
		auto writer = FlatVector::Writer<int64_t>(chunk.data[0], 2);
		writer.WriteNull();
		writer.WriteValue(12);
	}
	auto data = FlatVector::GetData(chunk.data[0]);
	REQUIRE(allocations.allocations == 1);
	for (idx_t iteration = 0; iteration < 3; iteration++) {
		chunk.Reset();
		auto writer = FlatVector::Writer<int64_t>(chunk.data[0], 2);
		writer.WriteValue(41);
		writer.WriteValue(42);
		REQUIRE(FlatVector::GetData(chunk.data[0]) == data);
		REQUIRE(chunk.data[0].GetValue(0) == Value::BIGINT(41));
	}
	REQUIRE(allocations.allocations == 1);
	REQUIRE(FlatVector::GetData(chunk.data[1]) == nullptr);
	chunk.Destroy();
	REQUIRE(allocations.live_bytes == 0);
}

TEST_CASE("Chunk reset preserves borrowed scalar buffers and wrappers", "[lazy_vector]") {
	Vector input(LogicalType::BIGINT);
	{
		auto writer = FlatVector::Writer<int64_t>(input, 2);
		writer.WriteValue(7);
		writer.WriteNull();
	}
	DataChunk output;
	output.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	SelectionVector sel(2);
	sel.set_index(0, 1);
	sel.set_index(1, 0);
	Vector dictionary(input, sel, 2);
	for (auto &source : {&input, &dictionary}) {
		output.data[0].Reference(*source);
		output.Reset();
		REQUIRE(source->size() == 2);
		REQUIRE(dictionary.GetValue(0).IsNull());
		REQUIRE(dictionary.GetValue(1) == Value::BIGINT(7));
		auto writer = FlatVector::Writer<int64_t>(output.data[0], 1);
		writer.WriteValue(99);
		REQUIRE(input.GetValue(0) == Value::BIGINT(7));
		REQUIRE(output.data[0].GetBufferRef() != source->GetBufferRef());
	}
	output.data[0].Slice(input, 0, 1);
	output.Reset();
	REQUIRE(input.size() == 2);
	REQUIRE(input.GetValue(0) == Value::BIGINT(7));
}

TEST_CASE("Active dictionary wrappers survive reset and are rebound", "[lazy_vector]") {
	DataChunk output;
	output.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	Vector input(LogicalType::BIGINT, 3);
	{
		auto writer = FlatVector::Writer<int64_t>(input, 3);
		writer.WriteValue(10);
		writer.WriteValue(20);
		writer.WriteValue(30);
	}
	SelectionVector sel(2);
	sel.set_index(0, 2);
	sel.set_index(1, 0);
	output.data[0].Slice(input, sel, 2);
	auto wrapper = output.data[0].GetBufferRef().get();
	duckdb::weak_ptr<VectorBuffer> retained = output.data[0].GetBufferRef();
	for (idx_t iteration = 0; iteration < 3; iteration++) {
		output.Reset();
		REQUIRE(output.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
		REQUIRE(output.data[0].GetBufferRef().get() == wrapper);
		REQUIRE(output.size() == 0);
		REQUIRE(DictionaryVector::Child(output.data[0]).size() == 0);
		REQUIRE_FALSE(DictionaryVector::DictionarySize(output.data[0]).IsValid());
		REQUIRE_FALSE(DictionaryVector::IsGlobalDictionary(output.data[0]));
		REQUIRE(DictionaryVector::DictionaryId(output.data[0]).empty());
		REQUIRE(output.GetAllocationSize() == 0);
		UnifiedVectorFormat format;
		output.data[0].ToUnifiedFormat(format);
		REQUIRE(output.data[0].Values<int64_t>().size() == 0);
		output.data[0].Verify();
		output.data[0].Slice(input, sel, 2);
		REQUIRE_FALSE(retained.expired());
		REQUIRE(output.data[0].GetBufferRef().get() == wrapper);
		REQUIRE(output.data[0].GetValue(0) == Value::BIGINT(30));
		REQUIRE(output.data[0].GetValue(1) == Value::BIGINT(10));
	}
	output.Reset();
	{
		auto writer = FlatVector::Writer<int64_t>(output.data[0], 1);
		writer.WriteValue(99);
	}
	REQUIRE(output.data[0].GetVectorType() == VectorType::FLAT_VECTOR);
	REQUIRE(retained.expired());
	REQUIRE(output.data[0].GetBufferRef().get() != wrapper);
	REQUIRE(input.GetValue(0) == Value::BIGINT(10));
}

TEST_CASE("Dictionary composition preserves shared selections and global entries", "[lazy_vector]") {
	auto entry = DictionaryVector::CreateReusableGlobalDictionary(LogicalType::BIGINT, 3);
	{
		auto writer = FlatVector::Writer<int64_t>(entry->data, 3);
		writer.WriteValue(10);
		writer.WriteValue(20);
		writer.WriteValue(30);
	}
	DataChunk input, output;
	input.InitializeEmpty({LogicalType::BIGINT, LogicalType::BIGINT});
	output.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT, LogicalType::BIGINT}, 3);
	SelectionVector original(3), select(2);
	original.set_index(0, 2);
	original.set_index(1, 0);
	original.set_index(2, 1);
	select.set_index(0, 2);
	select.set_index(1, 0);
	for (auto &column : input.data) {
		column.Dictionary(entry, original, 3);
	}
	output.Slice(input, select, 2);
	auto wrapper = output.data[0].GetBufferRef().get();
	duckdb::weak_ptr<VectorBuffer> retained = output.data[0].GetBufferRef();
	for (idx_t iteration = 0; iteration < 3; iteration++) {
		output.Reset();
		output.Slice(input, select, 2);
		REQUIRE_FALSE(retained.expired());
		REQUIRE(output.data[0].GetBufferRef().get() == wrapper);
		REQUIRE(DictionaryVector::SelVector(output.data[0]).data() ==
		        DictionaryVector::SelVector(output.data[1]).data());
		REQUIRE(DictionaryVector::IsGlobalDictionary(output.data[0]));
		REQUIRE(DictionaryVector::DictionaryId(output.data[0]) == entry->id);
		REQUIRE(output.GetValue(0, 0) == Value::BIGINT(20));
		REQUIRE(output.GetValue(1, 1) == Value::BIGINT(30));
		REQUIRE(input.data[0].size() == 3);
		REQUIRE(original.get_index(0) == 2);
	}
	output.Reset();
	input.Copy(output);
	REQUIRE(output.GetValue(0, 2) == Value::BIGINT(20));
	output.Reset();
	output.data[0].Slice(input.data[0], select, 2);
	output.Reset();
	output.SetChildCardinality(1);
	FlatVector::GetDataMutable<int64_t>(output.data[0])[0] = 9;
	REQUIRE(output.GetValue(0, 0) == Value::BIGINT(9));
}

TEST_CASE("Empty reset dictionaries preserve slicing ownership", "[lazy_vector]") {
	auto state = make_uniq<LazyAllocationState>();
	auto &allocations = *state;
	Allocator allocator(LazyAllocationState::Allocate, LazyAllocationState::Free, LazyAllocationState::Reallocate,
	                    std::move(state));
	DataChunk chunk;
	chunk.Initialize(allocator, {LogicalType::BIGINT}, 8);
	Vector input(Value::BIGINT(7), count_t(1));
	input.Flatten();
	SelectionVector sel(1);
	sel.set_index(0, 0);
	auto &source = chunk.data[0];
	source.Slice(input, sel, 1);
	chunk.Reset();
	auto owned_buffer = DictionaryVector::Child(source).GetBufferRef();
	REQUIRE(DictionaryVector::Child(source).GetType() == source.GetType());
	REQUIRE_FALSE(DictionaryVector::SelVector(source).IsSet());
	REQUIRE(allocations.allocations == 0);

	SECTION("Range slicing creates a separate zero-capacity view") {
		Vector result(source.GetType(), nullptr);
		result.Slice(source, 0, 0);
		REQUIRE(result.GetBufferRef() != owned_buffer);
		REQUIRE(FlatVector::GetCapacity(result) == 0);
		result.Append(Value::BIGINT(42), VectorAppendMode::ALLOW_RESIZE);
		REQUIRE(result.GetValue(0) == Value::BIGINT(42));
		REQUIRE(owned_buffer->Size() == 0);
	}
	SECTION("Selected slicing creates an ordinary dictionary") {
		auto result = Vector::Ref(source);
		result.Slice(sel, 0);
		REQUIRE(result.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		REQUIRE(result.GetBufferRef() != source.GetBufferRef());
		REQUIRE(result.Buffer().Cast<DictionaryBuffer>().GetEntryPtr());
		REQUIRE(result.size() == 0);
		REQUIRE(allocations.allocations == 0);
	}
	SECTION("Cached slicing does not cache an empty reset selection") {
		SelCache cache;
		auto result = Vector::Ref(source);
		result.Slice(sel, 0, cache);
		REQUIRE(cache.cache.empty());
		REQUIRE(result.Buffer().Cast<DictionaryBuffer>().GetEntryPtr());
		REQUIRE(result.size() == 0);
		REQUIRE(allocations.allocations == 0);
	}
	SECTION("Plain flatten takes the retained storage without allocating") {
		auto result = Vector::Ref(source);
		result.Flatten();
		REQUIRE(result.GetBufferRef() == owned_buffer);
		REQUIRE(FlatVector::GetCapacity(result) == 8);
		REQUIRE(allocations.allocations == 0);
	}
	SECTION("Selected flatten produces independent writable storage") {
		auto result = Vector::Ref(source);
		result.Flatten(sel, 0);
		REQUIRE(result.GetBufferRef() != owned_buffer);
		result.Append(Value::BIGINT(42), VectorAppendMode::ALLOW_RESIZE);
		REQUIRE(result.GetValue(0) == Value::BIGINT(42));
		REQUIRE(owned_buffer->Size() == 0);
	}
	SECTION("An empty range can be sliced in place") {
		source.Slice(source, 0, 0);
		REQUIRE(FlatVector::GetCapacity(source) == 0);
		REQUIRE(source.GetBufferRef() != owned_buffer);
	}
	SECTION("Empty serialization needs neither an input entry nor payload") {
		MemoryStream stream(Allocator::DefaultAllocator());
		BinarySerializer serializer(stream);
		serializer.Begin();
		source.Serialize(serializer);
		serializer.End();
		REQUIRE(allocations.allocations == 0);
		stream.Rewind();
		BinaryDeserializer deserializer(stream);
		deserializer.Begin();
		Vector result(source.GetType());
		result.Deserialize(deserializer, 0);
		deserializer.End();
		REQUIRE(result.size() == 0);
		REQUIRE(result.GetType() == source.GetType());
	}
	REQUIRE(source.size() == 0);
	REQUIRE(owned_buffer->Size() == 0);
}

TEST_CASE("Nested cache children remain eager and reusable with direct child writers", "[lazy_vector]") {
	auto structure = LogicalType::STRUCT({{"number", LogicalType::BIGINT}, {"text", LogicalType::VARCHAR}});
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {structure, LogicalType::LIST(LogicalType::BIGINT)}, 8);
	auto &fields = StructVector::GetEntries(chunk.data[0]);
	auto &elements = ListVector::GetChildMutable(chunk.data[1]);
	REQUIRE(FlatVector::GetData(fields[0]) != nullptr);
	REQUIRE(FlatVector::GetData(fields[1]) != nullptr);
	REQUIRE(FlatVector::GetData(elements) != nullptr);
	for (idx_t iteration = 0; iteration < 2; iteration++) {
		chunk.Reset();
		{
			auto writer = FlatVector::Writer<VectorStructType<int64_t, string_t>>(chunk.data[0], 1);
			writer.WriteValue([&](auto &number, auto &text) {
				number.WriteValue(42);
				text.WriteValue(string_t("a string longer than the inline storage"));
			});
			auto lists = FlatVector::Writer<VectorListType<int64_t>>(chunk.data[1], 1);
			for (auto &child : lists.WriteList(2)) {
				child.WriteValue(7);
			}
		}
		REQUIRE(chunk.size() == 1);
		REQUIRE(fields[0].GetValue(0) == Value::BIGINT(42));
		REQUIRE(fields[1].GetValue(0) == Value("a string longer than the inline storage"));
		REQUIRE(elements.size() == 2);
		REQUIRE(elements.GetValue(1) == Value::BIGINT(7));
	}
}

TEST_CASE("Reset dictionaries append using the owned chunk capacity", "[lazy_vector]") {
	DataChunk input, output;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT}, 8);
	output.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT}, 8);
	{
		auto writer = FlatVector::Writer<int64_t>(input.data[0], 1);
		writer.WriteValue(42);
	}
	SelectionVector sel(1);
	sel.set_index(0, 0);
	output.Slice(input, sel, 1);
	output.Reset();
	REQUIRE(output.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
	output.Append(input, VectorAppendMode::ERROR_ON_NO_SPACE);
	REQUIRE(output.size() == 1);
	REQUIRE(output.GetValue(0, 0) == Value::BIGINT(42));
}

TEST_CASE("Reset dictionaries retain the destination allocator and survive chunk destruction", "[lazy_vector]") {
	auto state = make_uniq<LazyAllocationState>();
	auto &allocations = *state;
	Allocator allocator(LazyAllocationState::Allocate, LazyAllocationState::Free, LazyAllocationState::Reallocate,
	                    std::move(state));
	DataChunk chunk;
	chunk.Initialize(allocator, {LogicalType::BIGINT}, 16);
	Vector input(Value::BIGINT(7), count_t(1));
	input.Flatten();
	SelectionVector sel(1);
	sel.set_index(0, 0);
	chunk.data[0].Slice(input, sel, 1);
	chunk.Reset();
	REQUIRE(allocations.allocations == 0);
	auto output = Vector::Ref(chunk.data[0]);
	chunk.Destroy();
	{
		auto writer = FlatVector::Writer<int64_t>(output, 1);
		writer.WriteValue(42);
	}
	REQUIRE(allocations.allocations == 1);
	REQUIRE(output.Buffer().GetAllocator().get() == &allocator);
	REQUIRE(output.GetValue(0) == Value::BIGINT(42));
}

TEST_CASE("Lazy scalar buffers handle failed allocation and capacity growth", "[lazy_vector]") {
	auto state = make_uniq<LazyAllocationState>();
	auto &allocations = *state;
	Allocator allocator(LazyAllocationState::Allocate, LazyAllocationState::Free, LazyAllocationState::Reallocate,
	                    std::move(state));
	DataChunk chunk;
	chunk.Initialize(allocator, {LogicalType::BIGINT}, 16);
	allocations.fail = true;
	REQUIRE_THROWS_AS(FlatVector::Writer<int64_t>(chunk.data[0], 1), OutOfMemoryException);
	REQUIRE(chunk.size() == 0);
	REQUIRE(FlatVector::GetData(chunk.data[0]) == nullptr);
	chunk.Reset();
	allocations.fail = false;
	chunk.data[0].Reserve(64);
	{
		auto writer = FlatVector::Writer<int64_t>(chunk.data[0], 1);
		writer.WriteValue(42);
	}
	REQUIRE(allocations.allocations == 1);
	chunk.Reset();
	REQUIRE(FlatVector::GetCapacity(chunk.data[0]) == 64);
	REQUIRE(chunk.GetCapacity() == 16);
	DataChunk moved;
	moved.Move(chunk);
	moved.Reset();
	REQUIRE(moved.data[0].Buffer().GetAllocator().get() == &allocator);
	moved.Destroy();
	REQUIRE(allocations.live_bytes == 0);

	DataChunk zero;
	zero.Initialize(allocator, {LogicalType::BIGINT}, 0);
	REQUIRE(allocations.allocations == 1);
	zero.data[0].Append(Value::BIGINT(55), VectorAppendMode::ALLOW_RESIZE);
	REQUIRE(zero.data[0].GetValue(0) == Value::BIGINT(55));
	REQUIRE(zero.data[0].Buffer().GetAllocator().get() == &allocator);
}

TEST_CASE("Lazy scalar mutable interfaces prepare their payload", "[lazy_vector]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT, LogicalType::VARCHAR});
	chunk.SetChildCardinality(2);
	FlatVector::GetDataMutable<int64_t>(chunk.data[0])[0] = 9;
	chunk.data[0].SetValue(1, Value::BIGINT(10));
	chunk.data[1].SetValue(0, Value(string(64, 'a')));
	chunk.data[1].SetValue(1, Value());
	REQUIRE(chunk.GetValue(0, 0) == Value::BIGINT(9));
	REQUIRE(chunk.GetValue(0, 1) == Value::BIGINT(10));
	REQUIRE(chunk.GetValue(1, 0) == Value(string(64, 'a')));
	REQUIRE(chunk.GetValue(1, 1).IsNull());
	Vector saved(LogicalType::VARCHAR);
	{
		auto writer = FlatVector::Writer<string_t>(saved, 1);
		writer.WriteStringRef(FlatVector::GetData<string_t>(chunk.data[1])[0]);
	}
	saved.AddHeapReference(chunk.data[1]);
	chunk.Reset();
	{
		auto writer = FlatVector::Writer<string_t>(chunk.data[1], 1);
		writer.WriteValue(string_t(string(64, 'b')));
	}
	REQUIRE(saved.GetValue(0) == Value(string(64, 'a')));
}

TEST_CASE("Reset dictionaries support flat mutation and empty reinterpretation", "[lazy_vector]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	Vector input(Value::BIGINT(7), count_t(1));
	input.Flatten();
	SelectionVector sel(1);
	sel.set_index(0, 0);
	chunk.data[0].Slice(input, sel, 1);
	chunk.Reset();
	Vector reinterpreted(LogicalType::UBIGINT, nullptr);
	reinterpreted.Reinterpret(chunk.data[0]);
	REQUIRE(reinterpreted.size() == 0);
	REQUIRE(chunk.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
	FlatVector::SetNull(chunk.data[0], 0, true);
	FlatVector::SetSize(chunk.data[0], 1);
	REQUIRE(chunk.data[0].GetValue(0).IsNull());
	chunk.data[0].Slice(input, sel, 1);
	chunk.Reset();
	int64_t external[] = {42};
	FlatVector::SetData(chunk.data[0], data_ptr_cast(external), count_t(1));
	REQUIRE(chunk.data[0].GetValue(0) == Value::BIGINT(42));
	chunk.data[0].Slice(input, sel, 1);
	chunk.Reset();
#ifndef DUCKDB_CRASH_ON_ASSERT
	Vector wrong_type(Value::INTEGER(12), count_t(1));
	wrong_type.Flatten();
	REQUIRE_THROWS_AS(chunk.data[0].Slice(wrong_type, sel, 1), InternalException);
#endif
	REQUIRE(chunk.data[0].size() == 0);
}

TEST_CASE("Zero-copy scalar data does not allocate a discarded payload", "[lazy_vector]") {
	auto state = make_uniq<LazyAllocationState>();
	auto &allocations = *state;
	Allocator allocator(LazyAllocationState::Allocate, LazyAllocationState::Free, LazyAllocationState::Reallocate,
	                    std::move(state));
	DataChunk chunk;
	chunk.Initialize(allocator, {LogicalType::BIGINT});
	int64_t external[] = {42, 43};
	for (idx_t iteration = 0; iteration < 3; iteration++) {
		chunk.Reset();
		FlatVector::SetData(chunk.data[0], data_ptr_cast(external), count_t(2));
		REQUIRE(chunk.data[0].GetValue(1) == Value::BIGINT(43));
		REQUIRE(allocations.allocations == 0);
	}
}
namespace {
struct LazySourceState : GlobalTableFunctionState {
	static constexpr idx_t BATCH_SIZE = STANDARD_VECTOR_SIZE < 32 ? STANDARD_VECTOR_SIZE : 32;
	idx_t batch = 0;
	buffer_ptr<VectorBuffer> previous;
};

void CheckLazyOutput(const Vector &result) {
	if (result.size() != 0) {
		throw InternalException("Execution output was not reset lazily");
	}
}

void LazySource(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<LazySourceState>();
	auto &result = output.data[0];
	CheckLazyOutput(result);
	if (state.batch == 0 && FlatVector::GetData(result)) {
		throw InternalException("Pipeline output allocated its payload before writing");
	}
	if (state.batch == 12) {
		return;
	}
	// Exercise the mutable-pointer boundary used by non-expression producers.
	auto data = FlatVector::GetDataMutable<int64_t>(result);
	std::fill(data, data + LazySourceState::BATCH_SIZE, NumericCast<int64_t>(state.batch));
	if (state.batch % 2 == 0) {
		FlatVector::ValidityMutable(result).SetInvalid(0);
	}
	FlatVector::SetSize(result, LazySourceState::BATCH_SIZE);
	if (result.Buffer().GetAllocator().get() != &BufferAllocator::Get(context) ||
	    (state.previous && state.previous != result.GetBufferRef())) {
		throw InternalException("Pipeline output lost its initialized resource or reusable storage");
	}
	state.previous = result.GetBufferRef();
	state.batch++;
}

void LazyReference(DataChunk &input, ExpressionState &, Vector &result) {
	CheckLazyOutput(result);
	if (FlatVector::GetData(result)) {
		throw InternalException("Reference-only scalar output allocated a payload");
	}
	result.Reference(input.data[0]);
}

void LazyComputed(DataChunk &input, ExpressionState &state, Vector &result) {
	CheckLazyOutput(result);
	UnaryExecutor::Execute<int64_t, int64_t>(input.data[0], result, input.size(),
	                                         [](int64_t value) { return value + 1; });
	if (result.Buffer().GetAllocator().get() != &BufferAllocator::Get(state.GetContext())) {
		throw InternalException("Expression output lost its initialized resource");
	}
}
} // namespace

TEST_CASE("Pipeline and expression outputs initialize and reset lazily", "[lazy_vector]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1"));
	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "lazy_output_test", ""};
	ExtensionLoader loader {load_info};
	TableFunction source(
	    "lazy_source", {}, LazySource,
	    [](ClientContext &, TableFunctionBindInput &, duckdb::vector<LogicalType> &types,
	       duckdb::vector<Identifier> &names) -> duckdb::unique_ptr<FunctionData> {
		    types.push_back(LogicalType::BIGINT);
		    names.push_back("i");
		    return nullptr;
	    },
	    [](ClientContext &, TableFunctionInitInput &) -> duckdb::unique_ptr<GlobalTableFunctionState> {
		    return make_uniq<LazySourceState>();
	    });
	loader.RegisterFunction(source);
	ScalarFunction reference("lazy_ref", {LogicalType::BIGINT}, LogicalType::BIGINT, LazyReference);
	reference.SetVolatile();
	loader.RegisterFunction(reference);
	ScalarFunction computed("lazy_computed", {LogicalType::BIGINT}, LogicalType::BIGINT, LazyComputed);
	computed.SetVolatile();
	loader.RegisterFunction(computed);

	for (idx_t execution = 0; execution < 3; execution++) {
		for (bool expressions : {false, true}) {
			auto query = expressions ? "SELECT lazy_ref(lazy_computed(lazy_ref(i))) FROM lazy_source()"
			                         : "SELECT * FROM lazy_source()";
			auto result = connection.Query(query);
			REQUIRE_NO_FAIL(*result);
			REQUIRE(result->RowCount() == 12 * LazySourceState::BATCH_SIZE);
			for (idx_t row = 0; row < result->RowCount(); row++) {
				auto expected =
				    row % (2 * LazySourceState::BATCH_SIZE) == 0
				        ? Value(LogicalType::BIGINT)
				        : Value::BIGINT(NumericCast<int64_t>(row / LazySourceState::BATCH_SIZE + expressions));
				REQUIRE(Value::NotDistinctFrom(result->Collection().GetValue(0, row), expected));
			}
		}
	}
}

TEST_CASE("Caching operators retain lazy output storage after slicing", "[lazy_vector]") {
	class SliceOperator : public CachingPhysicalOperator {
	public:
		explicit SliceOperator(PhysicalPlan &plan)
		    : CachingPhysicalOperator(plan, PhysicalOperatorType::FILTER, {LogicalType::BIGINT}, 1) {
		}

	protected:
		OperatorResultType ExecuteInternal(ExecutionContext &, DataChunk &input, DataChunk &output,
		                                   GlobalOperatorState &, OperatorState &) const override {
			SelectionVector selection(1);
			selection.set_index(0, 0);
			output.Slice(input, selection, 1);
			return OperatorResultType::NEED_MORE_INPUT;
		}
	};

	DuckDB db(nullptr);
	Connection connection(db);
	ThreadContext thread(*connection.context);
	ExecutionContext context(*connection.context, thread, nullptr);
	PhysicalPlan plan(Allocator::DefaultAllocator());
	SliceOperator op(plan);
	GlobalOperatorState global;
	CachingOperatorState state;
	state.initialized = true;
	state.can_cache_chunk = OperatorCachingMode::UNORDERED;
	DataChunk input, output;
	input.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	{
		auto writer = FlatVector::Writer<int64_t>(input.data[0], 1);
		writer.WriteValue(7);
	}
	auto &allocator = BufferAllocator::Get(*connection.context);
	output.Initialize(allocator, {LogicalType::BIGINT});
	auto owned_buffer = output.data[0].GetBufferRef();
	duckdb::weak_ptr<VectorBuffer> wrapper;
	const auto batch_count = MinValue<idx_t>(3, STANDARD_VECTOR_SIZE);
	for (idx_t iteration = 0; iteration < batch_count; iteration++) {
		REQUIRE(op.Execute(context, input, output, global, state) == OperatorResultType::NEED_MORE_INPUT);
		REQUIRE(output.size() == 0);
		REQUIRE(output.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
		auto &child = DictionaryVector::Child(output.data[0]);
		REQUIRE(child.GetBufferRef() == owned_buffer);
		REQUIRE(child.Buffer().GetAllocator().get() == &allocator);
		REQUIRE(FlatVector::GetData(child) == nullptr);
		if (iteration == 0) {
			wrapper = output.data[0].GetBufferRef();
		} else {
			REQUIRE(wrapper.lock() == output.data[0].GetBufferRef());
		}
		REQUIRE(state.cached_chunk->size() == iteration + 1);
		REQUIRE(state.cached_chunk->GetValue(0, iteration) == Value::BIGINT(7));
		REQUIRE(input.GetValue(0, 0) == Value::BIGINT(7));
	}
	REQUIRE(op.FinalExecute(context, output, global, state) == OperatorFinalizeResultType::FINISHED);
	REQUIRE(output.size() == batch_count);
	for (idx_t row = 0; row < output.size(); row++) {
		REQUIRE(output.GetValue(0, row) == Value::BIGINT(7));
	}
}

TEST_CASE("Heap references added after a dictionary reset survive the next writer", "[lazy_vector]") {
	struct LifetimeHolder : AuxiliaryDataHolder {
		explicit LifetimeHolder(bool &destroyed) : destroyed(destroyed) {
		}
		~LifetimeHolder() override {
			destroyed = true;
		}
		bool &destroyed;
	};
	bool destroyed = false;
	Vector input(Value(string(64, 'x')), count_t(1));
	input.Flatten();
	input.AddAuxiliaryData(make_uniq<LifetimeHolder>(destroyed));
	DataChunk output;
	output.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR});
	SelectionVector sel(1);
	sel.set_index(0, 0);
	output.data[0].Slice(input, sel, 1);
	output.Reset();
	output.data[0].AddHeapReference(input);
	{
		auto writer = FlatVector::Writer<string_t>(output.data[0], 1);
		writer.WriteStringRef(FlatVector::GetData<string_t>(input)[0]);
	}
	input.SetBuffer(nullptr);
	REQUIRE_FALSE(destroyed);
	REQUIRE(output.data[0].GetValue(0) == Value(string(64, 'x')));
	output.Destroy();
	REQUIRE(destroyed);
}

TEST_CASE("Null-only scalar producers have physical storage", "[lazy_vector]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	FlatVector::ValidityMutable(chunk.data[0]).SetAllInvalid(2);
	FlatVector::SetSize(chunk.data[0], 2);
	REQUIRE(FlatVector::GetData(chunk.data[0]) != nullptr);
	REQUIRE(chunk.data[0].GetValue(0).IsNull());
	REQUIRE(chunk.data[0].GetValue(1).IsNull());
}
