/* Copyright 2015 Google Inc. All Rights Reserved.

   Distributed under MIT license.
   See file LICENSE for detail or copy at https://opensource.org/licenses/MIT
*/

/* Hash table on the 4-byte prefixes of static dictionary words. */

#ifndef BROTLI_ENC_DICTIONARY_HASH_H_
#define BROTLI_ENC_DICTIONARY_HASH_H_

#include <brotli/types.h>

namespace duckdb_brotli {

extern const uint16_t kStaticDictionaryHashWords[32768];
extern const uint8_t kStaticDictionaryHashLengths[32768];

}

#endif  /* BROTLI_ENC_DICTIONARY_HASH_H_ */
