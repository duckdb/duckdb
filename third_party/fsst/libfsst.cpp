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
#include "libfsst.hpp"

Symbol concat(Symbol a, Symbol b) {
	Symbol s;
	u32 length = a.length()+b.length();
	if (length > Symbol::maxLength) length = Symbol::maxLength;
	s.set_code_len(FSST_CODE_MASK, length);
	s.val.num = (b.val.num << (8*a.length())) | a.val.num;
	return s;
}

namespace std {
template <>
class hash<QSymbol> {
public:
	size_t operator()(const QSymbol& q) const {
		uint64_t k = q.symbol.val.num;
		const uint64_t m = 0xc6a4a7935bd1e995;
		const int r = 47;
		uint64_t h = 0x8445d61a4e774912 ^ (8*m);
		k *= m;
		k ^= k >> r;
		k *= m;
		h ^= k;
		h *= m;
		h ^= h >> r;
		h *= m;
		h ^= h >> r;
		return h;
	}
};
}

bool isEscapeCode(u16 pos) { return pos < FSST_CODE_BASE; }

std::ostream& operator<<(std::ostream& out, const Symbol& s) {
	for (u32 i=0; i<s.length(); i++)
		out << s.val.str[i];
	return out;
}

SymbolTable *buildSymbolTable(Counters& counters, vector<u8*> line, size_t len[], bool zeroTerminated=false) {
	SymbolTable *st = new SymbolTable(), *bestTable = new SymbolTable();
	int bestGain = (int) -FSST_SAMPLEMAXSZ; // worst case (everything exception)
	size_t sampleFrac = 128;

	// start by determining the terminator. We use the (lowest) most infrequent byte as terminator
	st->zeroTerminated = zeroTerminated;
	if (zeroTerminated) {
		st->terminator = 0; // except in case of zeroTerminated mode, then byte 0 is terminator regardless frequency
	} else {
		u16 byteHisto[256];
		memset(byteHisto, 0, sizeof(byteHisto));
		for(size_t i=0; i<line.size(); i++) {
			u8* cur = line[i];
			u8* end = cur + len[i];
			while(cur < end) byteHisto[*cur++]++;
		}
		u32 minSize = FSST_SAMPLEMAXSZ, i = st->terminator = 256;
		while(i-- > 0) {
			if (byteHisto[i] > minSize) continue;
			st->terminator = i;
			minSize = byteHisto[i];
		}
	}
	assert(st->terminator != 256);

	// a random number between 0 and 128
	auto rnd128 = [&](size_t i) { return 1 + (FSST_HASH((i+1UL)*sampleFrac)&127); };

	// compress sample, and compute (pair-)frequencies
	auto compressCount = [&](SymbolTable *st, Counters &counters) { // returns gain
		int gain = 0;

		for(size_t i=0; i<line.size(); i++) {
			u8* cur = line[i];
			u8* end = cur + len[i];

			if (sampleFrac < 128) {
				// in earlier rounds (sampleFrac < 128) we skip data in the sample (reduces overall work ~2x)
				if (rnd128(i) > sampleFrac) continue;
			}
			if (cur < end) {
				u8* start = cur;
				u16 code2 = 255, code1 = st->findLongestSymbol(cur, end);
				cur += st->symbols[code1].length();
				gain += (int) (st->symbols[code1].length()-(1+isEscapeCode(code1)));
				while (true) {
					// count single symbol (i.e. an option is not extending it)
					counters.count1Inc(code1);

					// as an alternative, consider just using the next byte..
					if (st->symbols[code1].length() != 1) // .. but do not count single byte symbols doubly
						counters.count1Inc(*start);

					if (cur==end) {
						break;
					}

					// now match a new symbol
					start = cur;
					if (cur<end-7) {
						u64 word = fsst_unaligned_load(cur);
						size_t code = word & 0xFFFFFF;
						size_t idx = FSST_HASH(code)&(st->hashTabSize-1);
						Symbol s = st->hashTab[idx];
						code2 = st->shortCodes[word & 0xFFFF] & FSST_CODE_MASK;
						word &= (0xFFFFFFFFFFFFFFFF >> (u8) s.icl);
						if ((s.icl < FSST_ICL_FREE) & (s.val.num == word)) {
							code2 = s.code();
							cur += s.length();
						} else if (code2 >= FSST_CODE_BASE) {
							cur += 2;
						} else {
							code2 = st->byteCodes[word & 0xFF] & FSST_CODE_MASK;
							cur += 1;
						}
					} else {
						code2 = st->findLongestSymbol(cur, end);
						cur += st->symbols[code2].length();
					}

					// compute compressed output size
					gain += ((int) (cur-start))-(1+isEscapeCode(code2));

					// now count the subsequent two symbols we encode as an extension codesibility
					if (sampleFrac < 128) { // no need to count pairs in final round
						                    // consider the symbol that is the concatenation of the two last symbols
						counters.count2Inc(code1, code2);

						// as an alternative, consider just extending with the next byte..
						if ((cur-start) > 1)  // ..but do not count single byte extensions doubly
							counters.count2Inc(code1, *start);
					}
					code1 = code2;
				}
			}
		}
		return gain;
	};

	auto makeTable = [&](SymbolTable *st, Counters &counters) {
		// hashmap of c (needed because we can generate duplicate candidates)
		unordered_set<QSymbol> cands;

		// artificially make terminater the most frequent symbol so it gets included
		u16 terminator = st->nSymbols?FSST_CODE_BASE:st->terminator;
		counters.count1Set(terminator,65535);

		auto addOrInc = [&](unordered_set<QSymbol> &cands, Symbol s, u64 count) {
			if (count < (5*sampleFrac)/128) return; // improves both compression speed (less candidates), but also quality!!
			QSymbol q;
			q.symbol = s;
			q.gain = count * s.length();
			auto it = cands.find(q);
			if (it != cands.end()) {
				q.gain += (*it).gain;
				cands.erase(*it);
			}
			cands.insert(q);
		};

		// add candidate symbols based on counted frequency
		for (u32 pos1=0; pos1<FSST_CODE_BASE+(size_t) st->nSymbols; pos1++) {
			u32 cnt1 = counters.count1GetNext(pos1); // may advance pos1!!
			if (!cnt1) continue;

			// heuristic: promoting single-byte symbols (*8) helps reduce exception rates and increases [de]compression speed
			Symbol s1 = st->symbols[pos1];
			addOrInc(cands, s1, ((s1.length()==1)?8LL:1LL)*cnt1);

			if (sampleFrac >= 128 || // last round we do not create new (combined) symbols
			    s1.length() == Symbol::maxLength || // symbol cannot be extended
			    s1.val.str[0] == st->terminator) { // multi-byte symbols cannot contain the terminator byte
				continue;
			}
			for (u32 pos2=0; pos2<FSST_CODE_BASE+(size_t)st->nSymbols; pos2++) {
				u32 cnt2 = counters.count2GetNext(pos1, pos2); // may advance pos2!!
				if (!cnt2) continue;

				// create a new symbol
				Symbol s2 = st->symbols[pos2];
				Symbol s3 = concat(s1, s2);
				if (s2.val.str[0] != st->terminator) // multi-byte symbols cannot contain the terminator byte
					addOrInc(cands, s3, cnt2);
			}
		}

		// insert candidates into priority queue (by gain)
		auto cmpGn = [](const QSymbol& q1, const QSymbol& q2) { return (q1.gain < q2.gain) || (q1.gain == q2.gain && q1.symbol.val.num > q2.symbol.val.num); };
		priority_queue<QSymbol,vector<QSymbol>,decltype(cmpGn)> pq(cmpGn);
		for (auto& q : cands)
			pq.push(q);

		// Create new symbol map using best candidates
		st->clear();
		while (st->nSymbols < 255 && !pq.empty()) {
			QSymbol q = pq.top();
			pq.pop();
			st->add(q.symbol);
		}
	};

	u8 bestCounters[512*sizeof(u16)];
#ifdef NONOPT_FSST
	for(size_t frac : {127, 127, 127, 127, 127, 127, 127, 127, 127, 128}) {
		sampleFrac = frac;
#else
	for(sampleFrac=8; true; sampleFrac += 30) {
#endif
		memset(&counters, 0, sizeof(Counters));
		long gain = compressCount(st, counters);
		if (gain >= bestGain) { // a new best solution!
			counters.backup1(bestCounters);
			*bestTable = *st; bestGain = gain;
		}
		if (sampleFrac >= 128) break; // we do 5 rounds (sampleFrac=8,38,68,98,128)
		makeTable(st, counters);
	}
	delete st;
	counters.restore1(bestCounters);
	makeTable(bestTable, counters);
	bestTable->finalize(zeroTerminated); // renumber codes for more efficient compression
	return bestTable;
}

static inline size_t compressSIMD(SymbolTable &symbolTable, u8* symbolBase, size_t nlines, size_t len[], u8* line[], size_t size, u8* dst, size_t lenOut[], u8* strOut[], int unroll) {
	size_t curLine = 0, inOff = 0, outOff = 0, batchPos = 0, empty = 0, budget = size;
	u8 *lim = dst + size, *codeBase = symbolBase + (1<<18); // 512KB temp space for compressing 512 strings
	SIMDjob input[512];  // combined offsets of input strings (cur,end), and string #id (pos) and output (dst) pointer
	SIMDjob output[512]; // output are (pos:9,dst:19) end pointers (compute compressed length from this)
	size_t jobLine[512]; // for which line in the input sequence was this job (needed because we may split a line into multiple jobs)

	while (curLine < nlines && outOff <= (1<<19)) {
		size_t prevLine = curLine, chunk, curOff = 0;

		// bail out if the output buffer cannot hold the compressed next string fully
		if (((len[curLine]-curOff)*2 + 7) > budget) break; // see below for the +7
		else budget -= (len[curLine]-curOff)*2;

		strOut[curLine] = (u8*) 0;
		lenOut[curLine] = 0;

		do {
			do {
				chunk = len[curLine] - curOff;
				if (chunk > 511) {
					chunk = 511; // large strings need to be chopped up into segments of 511 bytes
				}
				// create a job in this batch
				SIMDjob job;
				job.cur = inOff;
				job.end = job.cur + chunk;
				job.pos = batchPos;
				job.out = outOff;

				// worst case estimate for compressed size (+7 is for the scatter that writes extra 7 zeros)
				outOff += 7 + 2*(size_t)(job.end - job.cur); // note, total size needed is 512*(511*2+7) bytes.
				if (outOff > (1<<19)) break; // simdbuf may get full, stop before this chunk

				// register job in this batch
				input[batchPos] = job;
				jobLine[batchPos] = curLine;

				if (chunk == 0) {
					empty++; // detect empty chunks -- SIMD code cannot handle empty strings, so they need to be filtered out
				} else {
					// copy string chunk into temp buffer
					memcpy(symbolBase + inOff, line[curLine] + curOff, chunk);
					inOff += chunk;
					curOff += chunk;
					symbolBase[inOff++] = (u8) symbolTable.terminator; // write an extra char at the end that will not be encoded
				}
				if (++batchPos == 512) break;
			} while(curOff < len[curLine]);

			if ((batchPos == 512) || (outOff > (1<<19)) || (++curLine >= nlines)) { // cannot accumulate more?
				if (batchPos-empty >= 32) { // if we have enough work, fire off fsst_compressAVX512 (32 is due to max 4x8 unrolling)
					// radix-sort jobs on length (longest string first)
					// -- this provides best load balancing and allows to skip empty jobs at the end
					u16 sortpos[513];
					memset(sortpos, 0, sizeof(sortpos));

					// calculate length histo
					for(size_t i=0; i<batchPos; i++) {
						size_t len = input[i].end - input[i].cur;
						sortpos[512UL - len]++;
					}
					// calculate running sum
					for(size_t i=1; i<=512; i++)
						sortpos[i] += sortpos[i-1];

					// move jobs to their final destination
					SIMDjob inputOrdered[512];
					for(size_t i=0; i<batchPos; i++) {
						size_t len = input[i].end - input[i].cur;
						size_t pos = sortpos[511UL - len]++;
						inputOrdered[pos] = input[i];
					}
					// finally.. SIMD compress max 256KB of simdbuf into (max) 512KB of simdbuf (but presumably much less..)
					for(size_t done = duckdb_fsst_compressAVX512(symbolTable, codeBase, symbolBase, inputOrdered, output, batchPos-empty, unroll);
					     done < batchPos; done++) output[done] = inputOrdered[done];
				} else {
					memcpy(output, input, batchPos*sizeof(SIMDjob));
				}

				// finish encoding (unfinished strings in process, plus the few last strings not yet processed)
				for(size_t i=0; i<batchPos; i++) {
					SIMDjob job = output[i];
					if (job.cur < job.end) { // finish encoding this string with scalar code
						u8* cur = symbolBase + job.cur;
						u8* end = symbolBase + job.end;
						u8* out = codeBase + job.out;
						while (cur < end) {
							u64 word = fsst_unaligned_load(cur);
							size_t code = symbolTable.shortCodes[word & 0xFFFF];
							size_t pos = word & 0xFFFFFF;
							size_t idx = FSST_HASH(pos)&(symbolTable.hashTabSize-1);
							Symbol s = symbolTable.hashTab[idx];
							out[1] = (u8) word; // speculatively write out escaped byte
							word &= (0xFFFFFFFFFFFFFFFF >> (u8) s.icl);
							if ((s.icl < FSST_ICL_FREE) && s.val.num == word) {
								*out++ = (u8) s.code(); cur += s.length();
							} else {
								// could be a 2-byte or 1-byte code, or miss
								// handle everything with predication
								*out = (u8) code;
								out += 1+((code&FSST_CODE_BASE)>>8);
								cur += (code>>FSST_LEN_BITS);
							}
						}
						job.out = out - codeBase;
					}
					// postprocess job info
					job.cur = 0;
					job.end = job.out - input[job.pos].out; // misuse .end field as compressed size
					job.out = input[job.pos].out; // reset offset to start of encoded string
					input[job.pos] = job;
				}

				// copy out the result data
				for(size_t i=0; i<batchPos; i++) {
					size_t lineNr = jobLine[i]; // the sort must be order-preserving, as we concatenate results string in order
					size_t sz = input[i].end; // had stored compressed lengths here
					if (!strOut[lineNr]) strOut[lineNr] = dst; // first segment will be the strOut pointer
					lenOut[lineNr] += sz; // add segment (lenOut starts at 0 for this reason)
					memcpy(dst, codeBase+input[i].out, sz);
					dst += sz;
				}

				// go for the next batch of 512 chunks
				inOff = outOff = batchPos = empty = 0;
				budget = (size_t) (lim - dst);
			}
		} while (curLine == prevLine && outOff <= (1<<19));
	}
	return curLine;
}


// optimized adaptive *scalar* compression method
static inline size_t compressBulk(SymbolTable &symbolTable, size_t nlines, size_t lenIn[], u8* strIn[], size_t size, u8* out, size_t lenOut[], u8* strOut[], bool noSuffixOpt, bool avoidBranch) {
	u8 *cur = NULL, *end =  NULL, *lim = out + size;
	size_t curLine, suffixLim = symbolTable.suffixLim;
	u8 byteLim = symbolTable.nSymbols + symbolTable.zeroTerminated - symbolTable.lenHisto[0];

	u8 buf[512+7] = {}; /* +7 sentinel is to avoid 8-byte unaligned-loads going beyond 511 out-of-bounds */

	// three variants are possible. dead code falls away since the bool arguments are constants
	auto compressVariant = [&](bool noSuffixOpt, bool avoidBranch) {
		while (cur < end) {
			u64 word = fsst_unaligned_load(cur);
			size_t code = symbolTable.shortCodes[word & 0xFFFF];
			if (noSuffixOpt && ((u8) code) < suffixLim) {
				// 2 byte code without having to worry about longer matches
				*out++ = (u8) code; cur += 2;
			} else {
				size_t pos = word & 0xFFFFFF;
				size_t idx = FSST_HASH(pos)&(symbolTable.hashTabSize-1);
				Symbol s = symbolTable.hashTab[idx];
				out[1] = (u8) word; // speculatively write out escaped byte
				word &= (0xFFFFFFFFFFFFFFFF >> (u8) s.icl);
				if ((s.icl < FSST_ICL_FREE) && s.val.num == word) {
					*out++ = (u8) s.code(); cur += s.length();
				} else if (avoidBranch) {
					// could be a 2-byte or 1-byte code, or miss
					// handle everything with predication
					*out = (u8) code;
					out += 1+((code&FSST_CODE_BASE)>>8);
					cur += (code>>FSST_LEN_BITS);
				} else if ((u8) code < byteLim) {
					// 2 byte code after checking there is no longer pattern
					*out++ = (u8) code; cur += 2;
				} else {
					// 1 byte code or miss.
					*out = (u8) code;
					out += 1+((code&FSST_CODE_BASE)>>8); // predicated - tested with a branch, that was always worse
					cur++;
				}
			}
		}
	};

	for(curLine=0; curLine<nlines; curLine++) {
		size_t chunk, curOff = 0;
		strOut[curLine] = out;
		do {
			cur = strIn[curLine] + curOff;
			chunk = lenIn[curLine] - curOff;
			if (chunk > 511) {
				chunk = 511; // we need to compress in chunks of 511 in order to be byte-compatible with simd-compressed FSST
			}
			if ((2*chunk+7) > (size_t) (lim-out)) {
				return curLine; // out of memory
			}
			// copy the string to the 511-byte buffer
			memcpy(buf, cur, chunk);
			buf[chunk] = (u8) symbolTable.terminator;
			cur = buf;
			end = cur + chunk;

			// based on symboltable stats, choose a variant that is nice to the branch predictor
			if (noSuffixOpt) {
				compressVariant(true,false);
			} else if (avoidBranch) {
				compressVariant(false,true);
			} else {
				compressVariant(false, false);
			}
		} while((curOff += chunk) < lenIn[curLine]);
		lenOut[curLine] = (size_t) (out - strOut[curLine]);
	}
	return curLine;
}

#define FSST_SAMPLELINE ((size_t) 512)

// quickly select a uniformly random set of lines such that we have between [FSST_SAMPLETARGET,FSST_SAMPLEMAXSZ) string bytes
vector<u8*> makeSample(u8* sampleBuf, u8* strIn[], size_t *lenIn, size_t nlines,
                                                    unique_ptr<vector<size_t>>& sample_len_out) {
	size_t totSize = 0;
	vector<u8*> sample;

	for(size_t i=0; i<nlines; i++)
		totSize += lenIn[i];
	if (totSize < FSST_SAMPLETARGET) {
		for(size_t i=0; i<nlines; i++)
			sample.push_back(strIn[i]);
	} else {
		size_t sampleRnd = FSST_HASH(4637947);
		u8* sampleLim = sampleBuf + FSST_SAMPLETARGET;

		sample_len_out = unique_ptr<vector<size_t>>(new vector<size_t>());
		sample_len_out->reserve(nlines + FSST_SAMPLEMAXSZ/FSST_SAMPLELINE);

		// This fails if we have a lot of small strings and a few big ones?
		while(sampleBuf < sampleLim) {
			// choose a non-empty line
			sampleRnd = FSST_HASH(sampleRnd);
			size_t linenr = sampleRnd % nlines;
			while (lenIn[linenr] == 0)
				if (++linenr == nlines) linenr = 0;

			// choose a chunk
			size_t chunks = 1 + ((lenIn[linenr]-1) / FSST_SAMPLELINE);
			sampleRnd = FSST_HASH(sampleRnd);
			size_t chunk = FSST_SAMPLELINE*(sampleRnd % chunks);

			// add the chunk to the sample
			size_t len = min(lenIn[linenr]-chunk,FSST_SAMPLELINE);
			memcpy(sampleBuf, strIn[linenr]+chunk, len);
			sample.push_back(sampleBuf);

			sample_len_out->push_back(len);
			sampleBuf += len;
		}
	}
	return sample;
}

extern "C" duckdb_fsst_encoder_t* duckdb_fsst_create(size_t n, size_t lenIn[], u8 *strIn[], int zeroTerminated) {
	u8* sampleBuf = new u8[FSST_SAMPLEMAXSZ];
	unique_ptr<vector<size_t>> sample_sizes;
	vector<u8*> sample = makeSample(sampleBuf, strIn, lenIn, n?n:1, sample_sizes); // careful handling of input to get a right-size and representative sample
	Encoder *encoder = new Encoder();
	size_t* sampleLen = sample_sizes ? sample_sizes->data() : &lenIn[0];
	encoder->symbolTable = shared_ptr<SymbolTable>(buildSymbolTable(encoder->counters, sample, sampleLen, zeroTerminated));
	delete[] sampleBuf;
	return (duckdb_fsst_encoder_t*) encoder;
}

/* create another encoder instance, necessary to do multi-threaded encoding using the same symbol table */
extern "C" duckdb_fsst_encoder_t* duckdb_fsst_duplicate(duckdb_fsst_encoder_t *encoder) {
	Encoder *e = new Encoder();
	e->symbolTable = ((Encoder*)encoder)->symbolTable; // it is a shared_ptr
	return (duckdb_fsst_encoder_t*) e;
}

// export a symbol table in compact format.
extern "C" u32 duckdb_fsst_export(duckdb_fsst_encoder_t *encoder, u8 *buf) {
	Encoder *e = (Encoder*) encoder;
	// In ->version there is a versionnr, but we hide also suffixLim/terminator/nSymbols there.
	// This is sufficient in principle to *reconstruct* a duckdb_fsst_encoder_t from a duckdb_fsst_decoder_t
	// (such functionality could be useful to append compressed data to an existing block).
	//
	// However, the hash function in the encoder hash table is endian-sensitive, and given its
	// 'lossy perfect' hashing scheme is *unable* to contain other-endian-produced symbol tables.
	// Doing a endian-conversion during hashing will be slow and self-defeating.
	//
	// Overall, we could support reconstructing an encoder for incremental compression, but
	// should enforce equal-endianness. Bit of a bummer. Not going there now.
	//
	// The version field is now there just for future-proofness, but not used yet

	// version allows keeping track of fsst versions, track endianness, and encoder reconstruction
	u64 version = (FSST_VERSION << 32) |  // version is 24 bits, most significant byte is 0
	              (((u64) e->symbolTable->suffixLim) << 24) |
	              (((u64) e->symbolTable->terminator) << 16) |
	              (((u64) e->symbolTable->nSymbols) << 8) |
	              FSST_ENDIAN_MARKER; // least significant byte is nonzero

	/* do not assume unaligned reads here */
	memcpy(buf, &version, 8);
	buf[8] = e->symbolTable->zeroTerminated;
	for(u32 i=0; i<8; i++)
		buf[9+i] = (u8) e->symbolTable->lenHisto[i];
	u32 pos = 17;

	// emit only the used bytes of the symbols
	for(u32 i = e->symbolTable->zeroTerminated; i < e->symbolTable->nSymbols; i++)
		for(u32 j = 0; j < e->symbolTable->symbols[i].length(); j++)
			buf[pos++] = e->symbolTable->symbols[i].val.str[j]; // serialize used symbol bytes

	return pos; // length of what was serialized
}

#define FSST_CORRUPT 32774747032022883 /* 7-byte number in little endian containing "corrupt" */

extern "C" u32 duckdb_fsst_import(duckdb_fsst_decoder_t *decoder, u8 *buf) {
	u64 version = 0;
	u32 code, pos = 17;
	u8 lenHisto[8];

	// version field (first 8 bytes) is now there just for future-proofness, unused still (skipped)
	memcpy(&version, buf, 8);
	if ((version>>32) != FSST_VERSION) return 0;
	decoder->zeroTerminated = buf[8]&1;
	memcpy(lenHisto, buf+9, 8);

	// in case of zero-terminated, first symbol is "" (zero always, may be overwritten)
	decoder->len[0] = 1;
	decoder->symbol[0] = 0;

	// we use lenHisto[0] as 1-byte symbol run length (at the end)
	code = decoder->zeroTerminated;
	if (decoder->zeroTerminated) lenHisto[0]--; // if zeroTerminated, then symbol "" aka 1-byte code=0, is not stored at the end

	// now get all symbols from the buffer
	for(u32 l=1; l<=8; l++) { /* l = 1,2,3,4,5,6,7,8 */
		for(u32 i=0; i < lenHisto[(l&7) /* 1,2,3,4,5,6,7,0 */]; i++, code++)  {
			decoder->len[code] = (l&7)+1; /* len = 2,3,4,5,6,7,8,1  */
			decoder->symbol[code] = 0;
			for(u32 j=0; j<decoder->len[code]; j++)
				((u8*) &decoder->symbol[code])[j] = buf[pos++]; // note this enforces 'little endian' symbols
		}
	}
	if (decoder->zeroTerminated) lenHisto[0]++;

	// fill unused symbols with text "corrupt". Gives a chance to detect corrupted code sequences (if there are unused symbols).
	while(code<255) {
		decoder->symbol[code] = FSST_CORRUPT;
		decoder->len[code++] = 8;
	}
	return pos;
}

// runtime check for simd
inline size_t _compressImpl(Encoder *e, size_t nlines, size_t lenIn[], u8 *strIn[], size_t size, u8 *output, size_t *lenOut, u8 *strOut[], bool noSuffixOpt, bool avoidBranch, int simd) {
#ifndef NONOPT_FSST
	if (simd && duckdb_fsst_hasAVX512())
		return compressSIMD(*e->symbolTable, e->simdbuf, nlines, lenIn, strIn, size, output, lenOut, strOut, simd);
#endif
	(void) simd;
	return compressBulk(*e->symbolTable, nlines, lenIn, strIn, size, output, lenOut, strOut, noSuffixOpt, avoidBranch);
}
size_t compressImpl(Encoder *e, size_t nlines, size_t lenIn[], u8 *strIn[], size_t size, u8 *output, size_t *lenOut, u8 *strOut[], bool noSuffixOpt, bool avoidBranch, int simd) {
	return _compressImpl(e, nlines, lenIn, strIn, size, output, lenOut, strOut, noSuffixOpt, avoidBranch, simd);
}

// adaptive choosing of scalar compression method based on symbol length histogram
inline size_t _compressAuto(Encoder *e, size_t nlines, size_t lenIn[], u8 *strIn[], size_t size, u8 *output, size_t *lenOut, u8 *strOut[], int simd) {
	bool avoidBranch = false, noSuffixOpt = false;
	if (100*e->symbolTable->lenHisto[1] > 65*e->symbolTable->nSymbols && 100*e->symbolTable->suffixLim > 95*e->symbolTable->lenHisto[1]) {
		noSuffixOpt = true;
	} else if ((e->symbolTable->lenHisto[0] > 24 && e->symbolTable->lenHisto[0] < 92) &&
	           (e->symbolTable->lenHisto[0] < 43 || e->symbolTable->lenHisto[6] + e->symbolTable->lenHisto[7] < 29) &&
	           (e->symbolTable->lenHisto[0] < 72 || e->symbolTable->lenHisto[2] < 72)) {
		avoidBranch = true;
	}
	return _compressImpl(e, nlines, lenIn, strIn, size, output, lenOut, strOut, noSuffixOpt, avoidBranch, simd);
}
size_t compressAuto(Encoder *e, size_t nlines, size_t lenIn[], u8 *strIn[], size_t size, u8 *output, size_t *lenOut, u8 *strOut[], int simd) {
	return _compressAuto(e, nlines, lenIn, strIn, size, output, lenOut, strOut, simd);
}

// the main compression function (everything automatic)
extern "C" size_t duckdb_fsst_compress(duckdb_fsst_encoder_t *encoder, size_t nlines, size_t lenIn[], u8 *strIn[], size_t size, u8 *output, size_t *lenOut, u8 *strOut[]) {
	// to be faster than scalar, simd needs 64 lines or more of length >=12; or fewer lines, but big ones (totLen > 32KB)
	size_t totLen = accumulate(lenIn, lenIn+nlines, 0);
	int simd = totLen > nlines*12 && (nlines > 64 || totLen > (size_t) 1<<15);
	return _compressAuto((Encoder*) encoder, nlines, lenIn, strIn, size, output, lenOut, strOut, 3*simd);
}

/* deallocate encoder */
extern "C" void duckdb_fsst_destroy(duckdb_fsst_encoder_t* encoder) {
	Encoder *e = (Encoder*) encoder;
	delete e;
}

/* very lazy implementation relying on export and import */
extern "C" duckdb_fsst_decoder_t duckdb_fsst_decoder(duckdb_fsst_encoder_t *encoder) {
	u8 buf[sizeof(duckdb_fsst_decoder_t)];
	u32 cnt1 = duckdb_fsst_export(encoder, buf);
	duckdb_fsst_decoder_t decoder;
	u32 cnt2 = duckdb_fsst_import(&decoder, buf);
	assert(cnt1 == cnt2); (void) cnt1; (void) cnt2;
	return decoder;
}