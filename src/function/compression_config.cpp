#include "duckdb/common/pair.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

typedef CompressionFunction (*get_compression_function_t)(PhysicalType type);
typedef bool (*compression_supports_type_t)(const PhysicalType physical_type);

struct DefaultCompressionMethod {
	CompressionType type;
	get_compression_function_t get_function;
	compression_supports_type_t supports_type;
};

static const DefaultCompressionMethod internal_compression_methods[] = {
    {CompressionType::COMPRESSION_CONSTANT, ConstantFun::GetFunction, ConstantFun::TypeIsSupported},
    {CompressionType::COMPRESSION_UNCOMPRESSED, UncompressedFun::GetFunction, UncompressedFun::TypeIsSupported},
    {CompressionType::COMPRESSION_RLE, RLEFun::GetFunction, RLEFun::TypeIsSupported},
    {CompressionType::COMPRESSION_BITPACKING, BitpackingFun::GetFunction, BitpackingFun::TypeIsSupported},
    {CompressionType::COMPRESSION_DICTIONARY, DictionaryCompressionFun::GetFunction,
     DictionaryCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_CHIMP, ChimpCompressionFun::GetFunction, ChimpCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_PATAS, PatasCompressionFun::GetFunction, PatasCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_ALP, AlpCompressionFun::GetFunction, AlpCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_ALPRD, AlpRDCompressionFun::GetFunction, AlpRDCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_FSST, FSSTFun::GetFunction, FSSTFun::TypeIsSupported},
    {CompressionType::COMPRESSION_ZSTD, ZSTDFun::GetFunction, ZSTDFun::TypeIsSupported},
    {CompressionType::COMPRESSION_ROARING, RoaringCompressionFun::GetFunction, RoaringCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_EMPTY, EmptyValidityCompressionFun::GetFunction,
     EmptyValidityCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_AUTO, nullptr, nullptr}};

static optional_ptr<CompressionFunction> FindCompressionFunction(CompressionFunctionSet &set, CompressionType type,
                                                                 const PhysicalType physical_type) {
	auto &functions = set.functions;
	auto comp_entry = functions.find(type);
	if (comp_entry != functions.end()) {
		auto &type_functions = comp_entry->second;
		auto type_entry = type_functions.find(physical_type);
		if (type_entry != type_functions.end()) {
			return &type_entry->second;
		}
	}
	return nullptr;
}

static optional_ptr<CompressionFunction> LoadCompressionFunction(CompressionFunctionSet &set, CompressionType type,
                                                                 const PhysicalType physical_type) {
	for (idx_t i = 0; internal_compression_methods[i].get_function; i++) {
		const auto &method = internal_compression_methods[i];
		if (method.type == type) {
			if (!method.supports_type(physical_type)) {
				return nullptr;
			}
			// The type is supported. We create the function and insert it into the set of available functions.
			auto function = method.get_function(physical_type);
			set.functions[type].insert(make_pair(physical_type, function));
			return FindCompressionFunction(set, type, physical_type);
		}
	}
	throw InternalException("Unsupported compression function type");
}

static void TryLoadCompression(DBConfig &config, vector<reference<CompressionFunction>> &result, CompressionType type,
                               const PhysicalType physical_type) {
	if (config.options.disabled_compression_methods.find(type) != config.options.disabled_compression_methods.end()) {
		// explicitly disabled
		return;
	}
	auto function = config.GetCompressionFunction(type, physical_type);
	if (!function) {
		return;
	}
	result.push_back(*function);
}

vector<reference<CompressionFunction>> DBConfig::GetCompressionFunctions(const PhysicalType physical_type) {
	vector<reference<CompressionFunction>> result;
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_UNCOMPRESSED, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_RLE, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_BITPACKING, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_DICTIONARY, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_CHIMP, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_PATAS, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_ALP, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_ALPRD, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_FSST, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_ZSTD, physical_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_ROARING, physical_type);
	return result;
}

optional_ptr<CompressionFunction> DBConfig::GetCompressionFunction(CompressionType type,
                                                                   const PhysicalType physical_type) {
	lock_guard<mutex> l(compression_functions->lock);

	// Check if the function is already loaded into the global compression functions.
	auto function = FindCompressionFunction(*compression_functions, type, physical_type);
	if (function) {
		return function;
	}

	// We could not find the function in the global compression functions,
	// so we attempt loading it.
	return LoadCompressionFunction(*compression_functions, type, physical_type);
}

} // namespace duckdb
