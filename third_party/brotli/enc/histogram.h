/* Copyright 2013 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* Models the histograms of literals, commands and distance codes. */

#ifndef BROTLI_ENC_HISTOGRAM_H_
#define BROTLI_ENC_HISTOGRAM_H_

#include <string.h>  /* memset */

#include <brotli/types.h>

#include "../common/brotli_constants.h"
#include "../common/context.h"
#include "../common/brotli_platform.h"
#include "block_splitter.h"
#include "command.h"

namespace duckdb_brotli {

/* The distance symbols effectively used by "Large Window Brotli" (32-bit). */
#define BROTLI_NUM_HISTOGRAM_DISTANCE_SYMBOLS 544

#define FN(X) X ## Literal
#define DATA_SIZE BROTLI_NUM_LITERAL_SYMBOLS
#define DataType uint8_t
/* NOLINT(build/header_guard) */
/* Copyright 2013 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: Histogram, DATA_SIZE, DataType */

/* A simple container for histograms of data in blocks. */

typedef struct FN(Histogram) {
  uint32_t data_[DATA_SIZE];
  size_t total_count_;
  double bit_cost_;
} FN(Histogram);

static BROTLI_INLINE void FN(HistogramClear)(FN(Histogram)* self) {
  memset(self->data_, 0, sizeof(self->data_));
  self->total_count_ = 0;
  self->bit_cost_ = HUGE_VAL;
}

static BROTLI_INLINE void FN(ClearHistograms)(
    FN(Histogram)* array, size_t length) {
  size_t i;
  for (i = 0; i < length; ++i) FN(HistogramClear)(array + i);
}

static BROTLI_INLINE void FN(HistogramAdd)(FN(Histogram)* self, size_t val) {
  ++self->data_[val];
  ++self->total_count_;
}

static BROTLI_INLINE void FN(HistogramAddVector)(FN(Histogram)* self,
    const DataType* p, size_t n) {
  self->total_count_ += n;
  n += 1;
  while (--n) ++self->data_[*p++];
}

static BROTLI_INLINE void FN(HistogramAddHistogram)(FN(Histogram)* self,
    const FN(Histogram)* v) {
  size_t i;
  self->total_count_ += v->total_count_;
  for (i = 0; i < DATA_SIZE; ++i) {
    self->data_[i] += v->data_[i];
  }
}

static BROTLI_INLINE size_t FN(HistogramDataSize)(void) { return DATA_SIZE; }
#undef DataType
#undef DATA_SIZE
#undef FN

#define FN(X) X ## Command
#define DataType uint16_t
#define DATA_SIZE BROTLI_NUM_COMMAND_SYMBOLS
/* NOLINT(build/header_guard) */
/* Copyright 2013 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: Histogram, DATA_SIZE, DataType */

/* A simple container for histograms of data in blocks. */

typedef struct FN(Histogram) {
  uint32_t data_[DATA_SIZE];
  size_t total_count_;
  double bit_cost_;
} FN(Histogram);

static BROTLI_INLINE void FN(HistogramClear)(FN(Histogram)* self) {
  memset(self->data_, 0, sizeof(self->data_));
  self->total_count_ = 0;
  self->bit_cost_ = HUGE_VAL;
}

static BROTLI_INLINE void FN(ClearHistograms)(
    FN(Histogram)* array, size_t length) {
  size_t i;
  for (i = 0; i < length; ++i) FN(HistogramClear)(array + i);
}

static BROTLI_INLINE void FN(HistogramAdd)(FN(Histogram)* self, size_t val) {
  ++self->data_[val];
  ++self->total_count_;
}

static BROTLI_INLINE void FN(HistogramAddVector)(FN(Histogram)* self,
    const DataType* p, size_t n) {
  self->total_count_ += n;
  n += 1;
  while (--n) ++self->data_[*p++];
}

static BROTLI_INLINE void FN(HistogramAddHistogram)(FN(Histogram)* self,
    const FN(Histogram)* v) {
  size_t i;
  self->total_count_ += v->total_count_;
  for (i = 0; i < DATA_SIZE; ++i) {
    self->data_[i] += v->data_[i];
  }
}

static BROTLI_INLINE size_t FN(HistogramDataSize)(void) { return DATA_SIZE; }
#undef DATA_SIZE
#undef FN

#define FN(X) X ## Distance
#define DATA_SIZE BROTLI_NUM_HISTOGRAM_DISTANCE_SYMBOLS
/* NOLINT(build/header_guard) */
/* Copyright 2013 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* template parameters: Histogram, DATA_SIZE, DataType */

/* A simple container for histograms of data in blocks. */

typedef struct FN(Histogram) {
  uint32_t data_[DATA_SIZE];
  size_t total_count_;
  double bit_cost_;
} FN(Histogram);

static BROTLI_INLINE void FN(HistogramClear)(FN(Histogram)* self) {
  memset(self->data_, 0, sizeof(self->data_));
  self->total_count_ = 0;
  self->bit_cost_ = HUGE_VAL;
}

static BROTLI_INLINE void FN(ClearHistograms)(
    FN(Histogram)* array, size_t length) {
  size_t i;
  for (i = 0; i < length; ++i) FN(HistogramClear)(array + i);
}

static BROTLI_INLINE void FN(HistogramAdd)(FN(Histogram)* self, size_t val) {
  ++self->data_[val];
  ++self->total_count_;
}

static BROTLI_INLINE void FN(HistogramAddVector)(FN(Histogram)* self,
    const DataType* p, size_t n) {
  self->total_count_ += n;
  n += 1;
  while (--n) ++self->data_[*p++];
}

static BROTLI_INLINE void FN(HistogramAddHistogram)(FN(Histogram)* self,
    const FN(Histogram)* v) {
  size_t i;
  self->total_count_ += v->total_count_;
  for (i = 0; i < DATA_SIZE; ++i) {
    self->data_[i] += v->data_[i];
  }
}

static BROTLI_INLINE size_t FN(HistogramDataSize)(void) { return DATA_SIZE; }
#undef DataType
#undef DATA_SIZE
#undef FN

BROTLI_INTERNAL void BrotliBuildHistogramsWithContext(
    const Command* cmds, const size_t num_commands,
    const BlockSplit* literal_split, const BlockSplit* insert_and_copy_split,
    const BlockSplit* dist_split, const uint8_t* ringbuffer, size_t pos,
    size_t mask, uint8_t prev_byte, uint8_t prev_byte2,
    const ContextType* context_modes, HistogramLiteral* literal_histograms,
    HistogramCommand* insert_and_copy_histograms,
    HistogramDistance* copy_dist_histograms);

}

#endif  /* BROTLI_ENC_HISTOGRAM_H_ */
