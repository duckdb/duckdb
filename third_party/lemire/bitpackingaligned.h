#pragma once

//#include "common.h"
#include <cinttypes>
#include <string>

namespace FastPForLib {

const uint32_t *fastunpack_8(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out, const uint32_t bit);
uint32_t *fastpackwithoutmask_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out, const uint32_t bit);

const uint32_t *fastunpack_16(const uint32_t *__restrict__ in,
                              uint32_t *__restrict__ out, const uint32_t bit);
uint32_t *fastpackwithoutmask_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out,
                                 const uint32_t bit);

const uint32_t *fastunpack_24(const uint32_t *__restrict__ in,
                              uint32_t *__restrict__ out, const uint32_t bit);
uint32_t *fastpackwithoutmask_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out,
                                 const uint32_t bit);

const uint32_t *fastunpack_32(const uint32_t *__restrict__ in,
                              uint32_t *__restrict__ out, const uint32_t bit);

uint32_t *fastpackwithoutmask_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out,
                                 const uint32_t bit);

} // namespace FastPFor
