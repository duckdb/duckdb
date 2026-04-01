#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/tablefilter_internal_functions.hpp"

namespace duckdb {

static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr idx_t MIN_NUM_BITS_PER_KEY = 12;
static constexpr idx_t MIN_NUM_BITS = 512;

void BloomFilter::Initialize(ClientContext &context_p, idx_t number_of_rows) {
	BufferManager &buffer_manager = BufferManager::GetBufferManager(context_p);

	const idx_t min_bits = MaxValue(MIN_NUM_BITS, number_of_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = MinValue(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
	bitmask = num_sectors - 1;

	buf_ = buffer_manager.GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint64_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	bf = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(bf, num_sectors, 0);

	initialized = true;
}

void BloomFilter::InsertHashes(const Vector &hashes_v, idx_t count) const {
	auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
	for (idx_t i = 0; i < count; i++) {
		InsertOne(hashes[i]);
	}
}

idx_t BloomFilter::LookupHashes(const Vector &hashes_v, SelectionVector &result_sel, const idx_t count) const {
	D_ASSERT(hashes_v.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes_v.GetType() == LogicalType::HASH);

	const auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
	idx_t found_count = 0;
	for (idx_t i = 0; i < count; i++) {
		result_sel.set_index(found_count, i);
		found_count += LookupOne(hashes[i]);
	}
	return found_count;
}

string BFTableFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("BFTableFilter");
}

FilterPropagateResult BFTableFilter::CheckStatistics(BaseStatistics &stats) const {
	TableFilter::ThrowDeprecated("BFTableFilter");
}

bool BFTableFilter::Equals(const TableFilter &other_p) const {
	TableFilter::ThrowDeprecated("BFTableFilter");
}
unique_ptr<TableFilter> BFTableFilter::Copy() const {
	TableFilter::ThrowDeprecated("BFTableFilter");
}

unique_ptr<Expression> BFTableFilter::ToExpression(const Expression &column) const {
	auto func = BloomFilterScalarFun::GetFunction(column.return_type);
	auto bind_data =
	    make_uniq<BloomFilterFunctionData>(filter, filters_null_values, key_column_name, key_type, 0.0f, idx_t(0));
	vector<unique_ptr<Expression>> args;
	args.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(func), std::move(args),
	                                          std::move(bind_data));
}

void BFTableFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
	serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(202, "key_type", key_type);
}

unique_ptr<TableFilter> BFTableFilter::Deserialize(Deserializer &deserializer) {
	auto filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
	auto key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");

	auto result = make_uniq<BFTableFilter>(nullptr, filters_null_values, key_column_name, key_type);
	return std::move(result);
}

} // namespace duckdb
