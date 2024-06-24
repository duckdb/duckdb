#include "duckdb/common/pair.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

typedef CompressionFunction (*get_compression_function_t)(PhysicalType type);
typedef bool (*compression_supports_type_t)(const CompressionInfo &info);

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
    {CompressionType::COMPRESSION_AUTO, nullptr, nullptr}};

static optional_ptr<CompressionFunction> FindCompressionFunction(CompressionFunctionSet &set, CompressionType type,
                                                                 const CompressionInfo &info) {
	auto &functions = set.functions;
	auto comp_entry = functions.find(type);
	if (comp_entry != functions.end()) {
		auto &type_functions = comp_entry->second;
		auto type_entry = type_functions.find(info.GetPhysicalType());
		if (type_entry != type_functions.end()) {
			return &type_entry->second;
		}
	}
	return nullptr;
}

static optional_ptr<CompressionFunction> LoadCompressionFunction(CompressionFunctionSet &set, CompressionType type,
                                                                 const CompressionInfo &info) {
	for (idx_t index = 0; internal_compression_methods[index].get_function; index++) {
		const auto &method = internal_compression_methods[index];
		if (method.type == type) {
			// found the correct compression type
			if (!method.supports_type(info)) {
				// but it does not support this data type: bail out
				return nullptr;
			}
			// the type is supported: create the function and insert it into the set
			auto function = method.get_function(info.GetPhysicalType());
			set.functions[type].insert(make_pair(info.GetPhysicalType(), function));
			return FindCompressionFunction(set, type, info);
		}
	}
	throw InternalException("Unsupported compression function type");
}

static void TryLoadCompression(DBConfig &config, vector<reference<CompressionFunction>> &result, CompressionType type,
                               const CompressionInfo &info) {
	auto function = config.GetCompressionFunction(type, info);
	if (!function) {
		return;
	}
	result.push_back(*function);
}

vector<reference<CompressionFunction>> DBConfig::GetCompressionFunctions(const CompressionInfo &info) {
	vector<reference<CompressionFunction>> result;
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_UNCOMPRESSED, info);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_RLE, info);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_BITPACKING, info);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_DICTIONARY, info);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_CHIMP, info);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_PATAS, info);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_ALP, info);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_ALPRD, info);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_FSST, info);
	return result;
}

optional_ptr<CompressionFunction> DBConfig::GetCompressionFunction(CompressionType type, const CompressionInfo &info) {
	lock_guard<mutex> l(compression_functions->lock);
	// check if the function is already loaded
	auto function = FindCompressionFunction(*compression_functions, type, info);
	if (function) {
		return function;
	}
	// else load the function
	return LoadCompressionFunction(*compression_functions, type, info);
}

} // namespace duckdb
