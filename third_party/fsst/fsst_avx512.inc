// this software is distributed under the MIT License (http://www.opensource.org/licenses/MIT):
//
// Copyright 2018-2020, CWI, TU Munich, FSU Jena
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files
// (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify,
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// - The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
// You can contact the authors via the FSST source repository : https://github.com/cwida/fsst
//
                        // load new jobs in the empty lanes (initially, all lanes are empty, so loadmaskX=11111111, deltaX=8).
            jobX      = _mm512_mask_expandloadu_epi64(jobX, loadmaskX, input); input += deltaX; 
                        // load the next 8 input string bytes (uncompressed data, aka 'symbols').
   __m512i  wordX     = _mm512_i64gather_epi64(_mm512_srli_epi64(jobX, 46), symbolBase, 1); 
                        // load 16-bits codes from the 2-byte-prefix keyed lookup table. It also store 1-byte codes in all free slots.
                        // codeX: Lowest 8 bits contain the code. Eleventh bit is whether it is an escaped code. Next 4 bits is length (2 or 1).
   __m512i  codeX     = _mm512_i64gather_epi64(_mm512_and_epi64(wordX, all_FFFF), symbolTable.shortCodes, sizeof(u16));
                        // get the first three bytes of the string. 
   __m512i  posX      = _mm512_mullo_epi64(_mm512_and_epi64(wordX, all_FFFFFF), all_PRIME);
                        // hash them into a random number: posX = posX*PRIME; posX ^= posX>>SHIFT
            posX      = _mm512_slli_epi64(_mm512_and_epi64(_mm512_xor_epi64(posX,_mm512_srli_epi64(posX,FSST_SHIFT)), all_HASH), 4);
                        // lookup in the 3-byte-prefix keyed hash table
   __m512i  iclX      = _mm512_i64gather_epi64(posX, (((char*) symbolTable.hashTab) + 8), 1);
                        // speculatively store the first input byte into the second position of the writeX register (in case it turns out to be an escaped byte).
   __m512i  writeX    = _mm512_slli_epi64(_mm512_and_epi64(wordX, all_FF), 8);
                        // lookup just like the iclX above, but loads the next 8 bytes. This fetches the actual string bytes in the hash table.
   __m512i  symbX     = _mm512_i64gather_epi64(posX, (((char*) symbolTable.hashTab) + 0), 1);
                        // generate the FF..FF mask with an FF for each byte of the symbol (we need to AND the input with this to correctly check equality).
            posX      = _mm512_srlv_epi64(all_MASK, _mm512_and_epi64(iclX, all_FF));
                        // check symbol < |str| as well as whether it is an occupied slot (cmplt checks both conditions at once) and check string equality (cmpeq).
   __mmask8 matchX    = _mm512_cmpeq_epi64_mask(symbX, _mm512_and_epi64(wordX, posX)) & _mm512_cmplt_epi64_mask(iclX, all_ICL_FREE);
                        // for the hits, overwrite the codes with what comes from the hash table (codes for symbols of length >=3). The rest stays with what shortCodes gave.
            codeX     = _mm512_mask_mov_epi64(codeX, matchX, _mm512_srli_epi64(iclX, 16));
                        // write out the code byte as the first output byte. Notice that this byte may also be the escape code 255 (for escapes) coming from shortCodes.
            writeX    = _mm512_or_epi64(writeX, _mm512_and_epi64(codeX, all_FF));
                        // zip the irrelevant 6 bytes (just stay with the 2 relevant bytes containing the 16-bits code)
            codeX     = _mm512_and_epi64(codeX, all_FFFF);
                        // write out the compressed data. It writes 8 bytes, but only 1 byte is relevant :-(or 2 bytes are, in case of an escape code)
                        _mm512_i64scatter_epi64(codeBase, _mm512_and_epi64(jobX, all_M19), writeX, 1);
                        // increase the jobX.cur field in the job with the symbol length (for this, shift away 12 bits from the code) 
            jobX      = _mm512_add_epi64(jobX, _mm512_slli_epi64(_mm512_srli_epi64(codeX, FSST_LEN_BITS), 46));
                        // increase the jobX.out' field with one, or two in case of an escape code (add 1 plus the escape bit, i.e the 8th)
            jobX      = _mm512_add_epi64(jobX, _mm512_add_epi64(all_ONE, _mm512_and_epi64(_mm512_srli_epi64(codeX, 8), all_ONE)));
                        // test which lanes are done now (jobX.cur==jobX.end), cur starts at bit 46, end starts at bit 28 (the highest 2x18 bits in the jobX register)
            loadmaskX = _mm512_cmpeq_epi64_mask(_mm512_srli_epi64(jobX, 46), _mm512_and_epi64(_mm512_srli_epi64(jobX, 28), all_M18));
                        // calculate the amount of lanes in jobX that are done
            deltaX    = _mm_popcnt_u32((int) loadmaskX); 
                        // write out the job state for the lanes that are done (we need the final 'jobX.out' value to compute the compressed string length)
                        _mm512_mask_compressstoreu_epi64(output, loadmaskX, jobX); output += deltaX;
