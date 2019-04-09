#include "execution/index/art/node16.hpp"

using namespace duckdb;

//TODO : In the future this can be performed using SIMD (#include <emmintrin.h>  x86 SSE intrinsics)
Node *Node16::getChild(const uint8_t k) const {
    for (uint32_t i = 0; i < count; ++i) {
        if (key[i] == k) {
            return child[i];
        }
    }
    return nullptr;
}
}