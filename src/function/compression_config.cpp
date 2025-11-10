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
    {CompressionType::COMPRESSION_DICT_FSST, DictFSSTCompressionFun::GetFunction,
     DictFSSTCompressionFun::TypeIsSupported},
    {CompressionType::COMPRESSION_AUTO, nullptr, nullptr}};

idx_t CompressionFunctionSet::GetCompressionIndex(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BOOL:
		return 0;
	case PhysicalType::UINT8:
		return 1;
	case PhysicalType::INT8:
		return 2;
	case PhysicalType::UINT16:
		return 3;
	case PhysicalType::INT16:
		return 4;
	case PhysicalType::UINT32:
		return 5;
	case PhysicalType::INT32:
		return 6;
	case PhysicalType::UINT64:
		return 7;
	case PhysicalType::INT64:
		return 8;
	case PhysicalType::FLOAT:
		return 9;
	case PhysicalType::DOUBLE:
		return 10;
	case PhysicalType::INTERVAL:
		return 11;
	case PhysicalType::LIST:
		return 12;
	case PhysicalType::STRUCT:
		return 13;
	case PhysicalType::ARRAY:
		return 14;
	case PhysicalType::VARCHAR:
		return 15;
	case PhysicalType::UINT128:
		return 16;
	case PhysicalType::INT128:
		return 17;
	case PhysicalType::BIT:
		return 18;
	default:
		throw InternalException("Unsupported physical type for compression index");
	}
}

idx_t CompressionFunctionSet::GetCompressionIndex(CompressionType type) {
	return static_cast<idx_t>(type);
}

CompressionFunctionSet::CompressionFunctionSet() {
	for (idx_t i = 0; i < PHYSICAL_TYPE_COUNT; i++) {
		is_loaded[i] = false;
	}
	ResetDisabledMethods();
	functions.resize(PHYSICAL_TYPE_COUNT);
}

bool EmitCompressionFunction(CompressionType type) {
	switch (type) {
	case CompressionType::COMPRESSION_UNCOMPRESSED:
	case CompressionType::COMPRESSION_RLE:
	case CompressionType::COMPRESSION_BITPACKING:
	case CompressionType::COMPRESSION_DICTIONARY:
	case CompressionType::COMPRESSION_CHIMP:
	case CompressionType::COMPRESSION_PATAS:
	case CompressionType::COMPRESSION_ALP:
	case CompressionType::COMPRESSION_ALPRD:
	case CompressionType::COMPRESSION_FSST:
	case CompressionType::COMPRESSION_ZSTD:
	case CompressionType::COMPRESSION_ROARING:
	case CompressionType::COMPRESSION_DICT_FSST:
		return true;
	default:
		return false;
	}
}

vector<reference<CompressionFunction>> CompressionFunctionSet::GetCompressionFunctions(PhysicalType physical_type) {
	LoadCompressionFunctions(physical_type);
	auto index = GetCompressionIndex(physical_type);
	auto &function_list = functions[index];
	vector<reference<CompressionFunction>> result;
	for (auto &entry : function_list) {
		auto compression_index = GetCompressionIndex(entry.type);
		if (is_disabled[compression_index]) {
			// explicitly disabled
			continue;
		}
		if (!EmitCompressionFunction(entry.type)) {
			continue;
		}
		result.push_back(entry);
	}
	return result;
}

void CompressionFunctionSet::LoadCompressionFunctions(PhysicalType physical_type) {
	auto index = GetCompressionIndex(physical_type);
	auto &function_list = functions[index];
	if (is_loaded[index]) {
		return;
	}
	// not loaded - try to load it
	lock_guard<mutex> guard(lock);
	// verify nobody loaded it in the mean-time
	if (is_loaded[index]) {
		return;
	}
	// actually perform the load
	for (idx_t i = 0; internal_compression_methods[i].get_function; i++) {
		TryLoadCompression(internal_compression_methods[i].type, physical_type, function_list);
	}
	is_loaded[index] = true;
}

void CompressionFunctionSet::TryLoadCompression(CompressionType type, PhysicalType physical_type,
                                                vector<CompressionFunction> &result) {
	for (idx_t i = 0; internal_compression_methods[i].get_function; i++) {
		const auto &method = internal_compression_methods[i];
		if (method.type == type) {
			if (!method.supports_type(physical_type)) {
				// not supported for this type
				return;
			}
			// The type is supported. We create the function and insert it into the set of available functions.
			result.push_back(method.get_function(physical_type));
			return;
		}
	}
	throw InternalException("Unsupported compression function type");
}

optional_ptr<CompressionFunction> CompressionFunctionSet::GetCompressionFunction(CompressionType type,
                                                                                 const PhysicalType physical_type) {
	LoadCompressionFunctions(physical_type);

	auto index = GetCompressionIndex(physical_type);
	auto &function_list = functions[index];
	for (auto &function : function_list) {
		if (function.type == type) {
			return function;
		}
	}
	return nullptr;
}

void CompressionFunctionSet::SetDisabledCompressionMethods(const vector<CompressionType> &methods) {
	ResetDisabledMethods();
	for (auto &method : methods) {
		auto idx = GetCompressionIndex(method);
		is_disabled[idx] = true;
	}
}

void DBConfig::SetDisabledCompressionMethods(const vector<CompressionType> &methods) {
	compression_functions->SetDisabledCompressionMethods(methods);
}

vector<CompressionType> CompressionFunctionSet::GetDisabledCompressionMethods() const {
	vector<CompressionType> result;
	for (idx_t i = 0; i < COMPRESSION_TYPE_COUNT; i++) {
		if (is_disabled[i]) {
			result.push_back(static_cast<CompressionType>(i));
		}
	}
	return result;
}

vector<CompressionType> DBConfig::GetDisabledCompressionMethods() const {
	return compression_functions->GetDisabledCompressionMethods();
}

void CompressionFunctionSet::ResetDisabledMethods() {
	for (idx_t i = 0; i < COMPRESSION_TYPE_COUNT; i++) {
		is_disabled[i] = false;
	}
}

vector<reference<CompressionFunction>> DBConfig::GetCompressionFunctions(const PhysicalType physical_type) {
	return compression_functions->GetCompressionFunctions(physical_type);
}

optional_ptr<CompressionFunction> DBConfig::GetCompressionFunction(CompressionType type,
                                                                   const PhysicalType physical_type) {
	return compression_functions->GetCompressionFunction(type, physical_type);
}

} // namespace duckdb
