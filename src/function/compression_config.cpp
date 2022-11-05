#include "duckdb/main/config.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

typedef CompressionFunction (*get_compression_function_t)(PhysicalType type);
typedef bool (*compression_supports_type_t)(PhysicalType type);

struct DefaultCompressionMethod {
	CompressionType type;
	get_compression_function_t get_function;
	compression_supports_type_t supports_type;
};

static DefaultCompressionMethod internal_compression_methods[] = {
    {CompressionType::COMPRESSION_CONSTANT, ConstantFun::GetFunction, ConstantFun::TypeIsSupported},
    {CompressionType::COMPRESSION_UNCOMPRESSED, UncompressedFun::GetFunction, UncompressedFun::TypeIsSupported},
    {CompressionType::COMPRESSION_RLE, RLEFun::GetFunction, RLEFun::TypeIsSupported},
    {CompressionType::COMPRESSION_BITPACKING, BitpackingFun::GetFunction, BitpackingFun::TypeIsSupported},
    {CompressionType::COMPRESSION_DICTIONARY, DictionaryCompressionFun::GetFunction,
     DictionaryCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_CHIMP, ChimpCompressionFun::GetFunction, ChimpCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_PATAS, PatasCompressionFun::GetFunction, PatasCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_FSST, FSSTFun::GetFunction, FSSTFun::TypeIsSupported},
    {CompressionType::COMPRESSION_AUTO, nullptr, nullptr}};

static CompressionFunction *FindCompressionFunction(CompressionFunctionSet &set, CompressionType type,
                                                    PhysicalType data_type) {
	auto &functions = set.functions;
	auto comp_entry = functions.find(type);
	if (comp_entry != functions.end()) {
		auto &type_functions = comp_entry->second;
		auto type_entry = type_functions.find(data_type);
		if (type_entry != type_functions.end()) {
			return &type_entry->second;
		}
	}
	return nullptr;
}

static CompressionFunction *LoadCompressionFunction(CompressionFunctionSet &set, CompressionType type,
                                                    PhysicalType data_type) {
	for (idx_t index = 0; internal_compression_methods[index].get_function; index++) {
		const auto &method = internal_compression_methods[index];
		if (method.type == type) {
			// found the correct compression type
			if (!method.supports_type(data_type)) {
				// but it does not support this data type: bail out
				return nullptr;
			}
			// the type is supported: create the function and insert it into the set
			auto function = method.get_function(data_type);
			set.functions[type].insert(make_pair(data_type, function));
			return FindCompressionFunction(set, type, data_type);
		}
	}
	throw InternalException("Unsupported compression function type");
}

static void TryLoadCompression(DBConfig &config, vector<CompressionFunction *> &result, CompressionType type,
                               PhysicalType data_type) {
	auto function = config.GetCompressionFunction(type, data_type);
	if (!function) {
		return;
	}
	result.push_back(function);
}

vector<CompressionFunction *> DBConfig::GetCompressionFunctions(PhysicalType data_type) {
	vector<CompressionFunction *> result;
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_UNCOMPRESSED, data_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_RLE, data_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_BITPACKING, data_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_DICTIONARY, data_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_CHIMP, data_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_PATAS, data_type);
	TryLoadCompression(*this, result, CompressionType::COMPRESSION_FSST, data_type);
	return result;
}

CompressionFunction *DBConfig::GetCompressionFunction(CompressionType type, PhysicalType data_type) {
	lock_guard<mutex> l(compression_functions->lock);
	// check if the function is already loaded
	auto function = FindCompressionFunction(*compression_functions, type, data_type);
	if (function) {
		return function;
	}
	// else load the function
	return LoadCompressionFunction(*compression_functions, type, data_type);
}

} // namespace duckdb
