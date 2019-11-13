#include <cfloat>
#include "execution/index/art/art_key.hpp"
#include "execution/index/art/art.hpp"

using namespace duckdb;

//! these are optimized and assume a particular byte order
#define BSWAP16(x) ((uint16_t)((((uint16_t)(x)&0xff00) >> 8) | (((uint16_t)(x)&0x00ff) << 8)))

#define BSWAP32(x)                                                                                                     \
	((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) | (((uint32_t)(x)&0x00ff0000) >> 8) |                               \
	            (((uint32_t)(x)&0x0000ff00) << 8) | (((uint32_t)(x)&0x000000ff) << 24)))

#define BSWAP64(x)                                                                                                     \
	((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) | (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |        \
	            (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) | (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |         \
	            (((uint64_t)(x)&0x00000000ff000000ull) << 8) | (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |         \
	            (((uint64_t)(x)&0x000000000000ff00ull) << 40) | (((uint64_t)(x)&0x00000000000000ffull) << 56)))

static uint8_t FlipSign(uint8_t key_byte) {
	return key_byte ^ 128;
}

uint32_t EncodeFloat(float x)
{
    int shift;
    unsigned long sign, exp, hibits, buff;
    double fnorm, significand;
    int expbits = 8;
    int significandbits = 23;

    //! zero (can't handle signed zero)
    if (x == 0)
    {
        buff = 0;
        return buff;
    }
    //! infinity
    if (x > FLT_MAX)
    {
        buff = 128 + ((1 << (expbits - 1)) - 1);
        buff <<= (31 - expbits);
        return buff;
    }
    //! -infinity
    if (x < -FLT_MAX)
    {
        buff = 128 + ((1 << (expbits - 1)) - 1);
        buff <<= (31 - expbits);
        buff |= (1 << 31);
        return buff;
    }
    //! NaN
    if (x != x)
    {
        buff = 128 + ((1 << (expbits - 1)) - 1);
        buff <<= (31 - expbits);
        buff |= 1234;
        return buff;
    }

    //! get the sign
    if (x < 0) { sign = 1; fnorm = -x; }
    else { sign = 0; fnorm = x; }

    //! get the normalized form of f and track the exponent
    shift = 0;
    while (fnorm >= 2.0) { fnorm /= 2.0; shift++; }
    while (fnorm < 1.0) { fnorm *= 2.0; shift--; }

    //! check for denormalized numbers
    if (shift < -126)
    {
        while (shift < -126) { fnorm /= 2.0; shift++; }
        shift = -1023;
    }
        //! out of range. Set to infinity
    else if (shift > 128)
    {
        buff = 128 + ((1 << (expbits - 1)) - 1);
        buff <<= (31 - expbits);
        buff |= (sign << 31);
        return buff;
    }
    else
        fnorm = fnorm - 1.0; //! take the significant bit off mantissa

    //! calculate the integer form of the significand
    //! hold it in a  double for now

    significand = fnorm * ((1LL << significandbits) + 0.5f);


    //! get the biased exponent
    exp = shift + ((1 << (expbits - 1)) - 1); //! shift + bias

    hibits = (long)(significand);
    buff = (sign << 31) | (exp << (31 - expbits)) | hibits;

    return buff;
}


uint64_t EncodeDouble(double x) {
    int shift;
    unsigned long sign, exp, hibits, hilong, lowlong;
    double fnorm, significand;
    int expbits = 11;
    int significandbits = 52;
    uint64_t buff;
    //! zero (can't handle signed zero)
    if (x == 0)
    {
        buff = 0;
        return buff;
    }
    //! infinity
    if (x > DBL_MAX)
    {
        hilong = 1024 + ((1 << (expbits - 1)) - 1);
        hilong <<= (31 - expbits);
        lowlong = 0;
        buff = hilong;
        buff <<=32;
        buff += lowlong;
        return buff;
    }
    //! -infinity
    if (x < -DBL_MAX)
    {
        hilong = 1024 + ((1 << (expbits - 1)) - 1);
        hilong <<= (31 - expbits);
        hilong |= (1 << 31);
        lowlong = 0;
        buff = hilong;
        buff <<=32;
        buff += lowlong;
        return buff;
    }
    //! NaN
    if (x != x)
    {
        hilong = 1024 + ((1 << (expbits - 1)) - 1);
        hilong <<= (31 - expbits);
        lowlong = 1234;
        buff = hilong;
        buff <<=32;
        buff += lowlong;
        return buff;
    }

    //! get the sign
    if (x < 0) { sign = 1; fnorm = -x; }
    else { sign = 0; fnorm = x; }

    //! get the normalized form of f and track the exponent
    shift = 0;
    while (fnorm >= 2.0) { fnorm /= 2.0; shift++; }
    while (fnorm < 1.0) { fnorm *= 2.0; shift--; }

    //! check for denormalized numbers
    if (shift < -1022)
    {
        while (shift < -1022) { fnorm /= 2.0; shift++; }
        shift = -1023;
    }
        //! out of range. Set to infinity
    else if (shift > 1023)
    {
        hilong = 1024 + ((1 << (expbits - 1)) - 1);
        hilong <<= (31 - expbits);
        hilong |= (sign << 31);
        lowlong = 0;
        buff = hilong;
        buff <<=32;
        buff += lowlong;
        return buff;
    }
    else
        fnorm = fnorm - 1.0; //! take the significant bit off mantissa

    //! calculate the integer form of the significand
    //! hold it in a  double for now

    significand = fnorm * ((1LL << significandbits) + 0.5f);


    //! get the biased exponent
    exp = shift + ((1 << (expbits - 1)) - 1); //! shift + bias

    //! put the data into two longs (for convenience)
    hibits = (long)(significand / 4294967296);
    hilong = (sign << 31) | (exp << (31 - expbits)) | hibits;
    //x = significand - hibits * 4294967296;
    lowlong = (unsigned long)(significand - hibits * 4294967296);

    buff = hilong;
    buff <<=32;
    buff += lowlong;
    return buff;
}


Key::Key(unique_ptr<data_t[]> data, index_t len) : len(len), data(move(data)) {
}

template <> unique_ptr<data_t[]> Key::CreateData(int8_t value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	reinterpret_cast<uint8_t *>(data.get())[0] = value;
	data[0] = FlipSign(data[0]);
	return data;
}

template <> unique_ptr<data_t[]> Key::CreateData(int16_t value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	reinterpret_cast<uint16_t *>(data.get())[0] = is_little_endian ? BSWAP16(value) : value;
	data[0] = FlipSign(data[0]);
	return data;
}

template <> unique_ptr<data_t[]> Key::CreateData(int32_t value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	reinterpret_cast<uint32_t *>(data.get())[0] = is_little_endian ? BSWAP32(value) : value;
	data[0] = FlipSign(data[0]);
	return data;
}

template <> unique_ptr<data_t[]> Key::CreateData(int64_t value, bool is_little_endian) {
	auto data = unique_ptr<data_t[]>(new data_t[sizeof(value)]);
	reinterpret_cast<uint64_t *>(data.get())[0] = is_little_endian ? BSWAP64(value) : value;
	data[0] = FlipSign(data[0]);
	return data;
}

template <> unique_ptr<data_t[]> Key::CreateData(float value, bool is_little_endian) {
    uint32_t converted_value = EncodeFloat(value);
    auto data = unique_ptr<data_t[]>(new data_t[sizeof(converted_value)]);
    reinterpret_cast<uint32_t *>(data.get())[0] = is_little_endian ? BSWAP32(converted_value) : converted_value;
    data[0] = FlipSign(data[0]);
    return data;
}
template <> unique_ptr<data_t[]> Key::CreateData(double value, bool is_little_endian) {
    uint64_t converted_value = EncodeDouble(value);
    auto data = unique_ptr<data_t[]>(new data_t[sizeof(converted_value)]);
    reinterpret_cast<uint64_t *>(data.get())[0] = is_little_endian ? BSWAP64(converted_value) : converted_value;
    data[0] = FlipSign(data[0]);
    return data;
}



template <> unique_ptr<Key> Key::CreateKey(string value, bool is_little_endian) {
	index_t len = value.size() + 1;
	auto data = unique_ptr<data_t[]>(new data_t[len]);
	memcpy(data.get(), value.c_str(), len);
	return make_unique<Key>(move(data), len);
}

bool Key::operator>(const Key &k) const {
	for (index_t i = 0; i < std::min(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len > k.len;
}

bool Key::operator>=(const Key &k) const {
	for (index_t i = 0; i < std::min(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len >= k.len;
}

bool Key::operator==(const Key &k) const {
	if (len != k.len) {
		return false;
	}
	for (index_t i = 0; i < len; i++) {
		if (data[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}

inline uint8_t &Key::operator[](std::size_t i) {
	assert(i <= len);
	return data[i];
}

inline const uint8_t &Key::operator[](std::size_t i) const {
	assert(i <= len);
	return data[i];
}
