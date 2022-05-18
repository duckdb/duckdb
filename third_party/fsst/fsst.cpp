// this software is distributed under the MIT License (http://www.opensource.org/licenses/MIT):
//
// Copyright 2018-2019, CWI, TU Munich, FSU Jena
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
#include "fsst.h" // the official FSST API -- also usable by C mortals
#include <condition_variable>
#include <iostream>
#include <fstream>
#include <mutex>
#include <vector>
#include <thread>
using namespace std;

// Utility to compress and decompress (-d) data with FSST (using stdin and stdout).
//
// The utility has a poor-man's async I/O in that it uses double buffering for input and output,
// and two background pthreads for reading and writing. The idea is to make the CPU overlap with I/O.
//
// The data format is quite simple. A FSST compressed file is a sequence of blocks, each with format:
// (1) 3-byte block length field (max blocksize is hence 16MB). This byte-length includes (1), (2) and (3).
// (2) FSST dictionary as produced by fst_export().
// (3) the FSST compressed data.
//
// The natural strength of FSST is in fact not block-based compression, but rather the compression and
// *individual* decompression of many small strings separately. Think of compressed databases and (column-store)
// data formats. But, this utility is to serve as an apples-to-apples comparison point with utilities like lz4.

namespace {

class BinarySemaphore {
   private:
   mutex m;
   condition_variable cv;
   bool value;

   public:
   explicit BinarySemaphore(bool initialValue = false) : value(initialValue) {}
   void wait() {
      unique_lock<mutex> lock(m);
      while (!value) cv.wait(lock);
      value = false;
   }
   void post() {
      { unique_lock<mutex> lock(m); value = true; }
      cv.notify_one();
   }
};

bool stopThreads = false;
BinarySemaphore srcDoneIO[2], dstDoneIO[2], srcDoneCPU[2], dstDoneCPU[2];
unsigned char *srcBuf[2] = { NULL, NULL };
unsigned char *dstBuf[2] = { NULL, NULL };
unsigned char *dstMem[2] = { NULL, NULL };
size_t srcLen[2] = { 0, 0 };
size_t dstLen[2] = { 0, 0 };

#define FSST_MEMBUF (1ULL<<22)
int decompress = 0;
size_t blksz = FSST_MEMBUF-(1+FSST_MAXHEADER/2); // block size of compression (max compressed size must fit 3 bytes)

#define DESERIALIZE(p) (((unsigned long long) (p)[0]) << 16) | (((unsigned long long) (p)[1]) << 8) | ((unsigned long long) (p)[2])
#define SERIALIZE(l,p) { (p)[0] = ((l)>>16)&255; (p)[1] = ((l)>>8)&255; (p)[2] = (l)&255; }

void reader(ifstream& src) {
   for(int swap=0; true; swap = 1-swap) {
      srcDoneCPU[swap].wait();
      if (stopThreads) break;
      src.read((char*) srcBuf[swap], blksz);
      srcLen[swap] = (unsigned long) src.gcount();
      if (decompress) {
         if (blksz && srcLen[swap] == blksz) {
            blksz = DESERIALIZE(srcBuf[swap]+blksz-3); // read size of next block
            srcLen[swap] -= 3; // cut off size bytes
         } else {
            blksz = 0;
         }
      }
      srcDoneIO[swap].post();
   }
}

void writer(ofstream& dst) {
   for(int swap=0; true; swap = 1-swap) {
      dstDoneCPU[swap].wait();
      if (!dstLen[swap]) break;
      dst.write((char*) dstBuf[swap], dstLen[swap]);
      dstDoneIO[swap].post();
   }
   for(int swap=0; swap<2; swap++)
      dstDoneIO[swap].post();
}

}

int main(int argc, char* argv[]) {
   size_t srcTot = 0, dstTot = 0;
   if (argc < 2 || argc > 4 || (argc == 4 && (argv[1][0] != '-' || argv[1][1] != 'd' || argv[1][2]))) {
      cerr << "usage: " << argv[0] << " -d infile outfile" << endl;
      cerr << "       " << argv[0] << " infile outfile" << endl;
      cerr << "       " << argv[0] << " infile" << endl;
      return -1;
   }
   decompress = (argc == 4);
   string srcfile(argv[1+decompress]), dstfile;
   if (argc == 2) {
      dstfile = srcfile + ".fsst";
   } else {
      dstfile = argv[2+decompress];
   }
   ifstream src;
   ofstream dst;
   src.open(srcfile, ios::binary);
   dst.open(dstfile, ios::binary);
   dst.exceptions(ios_base::failbit);
   dst.exceptions(ios_base::badbit);
   src.exceptions(ios_base::badbit);
   if (decompress) {
       unsigned char tmp[3];
       src.read((char*) tmp, 3);
       if (src.gcount() != 3) {
          cerr << "failed to open input." << endl;
          return -1;
       }
       blksz = DESERIALIZE(tmp); // read first block size
   }
   vector<unsigned char> buffer(FSST_MEMBUF*6);
   srcBuf[0] = buffer.data();
   srcBuf[1] = srcBuf[0] + (FSST_MEMBUF*(1ULL+decompress));
   dstMem[0] = srcBuf[1] + (FSST_MEMBUF*(1ULL+decompress));
   dstMem[1] = dstMem[0] + (FSST_MEMBUF*(2ULL-decompress));

   for(int swap=0; swap<2; swap++) {
      srcDoneCPU[swap].post(); // input buffer is not being processed initially
      dstDoneIO[swap].post();  // output buffer is not being written initially
   }
   thread readerThread([&src]{ reader(src); });
   thread writerThread([&dst]{ writer(dst); });

   for(int swap=0; true; swap = 1-swap) {
      srcDoneIO[swap].wait(); // wait until input buffer is available (i.e. done reading)
      dstDoneIO[swap].wait(); // wait until output buffer is ready writing hence free for use
      if (srcLen[swap] == 0) {
         dstLen[swap] = 0;
         break;
      }
      if (decompress) {
          fsst_decoder_t decoder;
          size_t hdr = fsst_import(&decoder, srcBuf[swap]);
          dstLen[swap] = fsst_decompress(&decoder, srcLen[swap] - hdr, srcBuf[swap] + hdr, FSST_MEMBUF, dstBuf[swap] = dstMem[swap]);
      } else {
          unsigned char tmp[FSST_MAXHEADER];
          fsst_encoder_t *encoder = fsst_create(1, &srcLen[swap], &srcBuf[swap], 0);
          size_t hdr = fsst_export(encoder, tmp);
          if (fsst_compress(encoder, 1, &srcLen[swap], &srcBuf[swap], FSST_MEMBUF*2, dstMem[swap]+FSST_MAXHEADER+3,
                                       &dstLen[swap], &dstBuf[swap]) < 1) return -1;
          dstLen[swap] += 3 + hdr;
          dstBuf[swap] -= 3 + hdr;
          SERIALIZE(dstLen[swap],dstBuf[swap]); // block starts with size
          copy(tmp, tmp+hdr, dstBuf[swap]+3); // then the header (followed by the compressed bytes which are already there)
          fsst_destroy(encoder);
      }
      srcTot += srcLen[swap];
      dstTot += dstLen[swap];
      srcDoneCPU[swap].post(); // input buffer may be re-used by the reader for the next block
      dstDoneCPU[swap].post(); // output buffer is ready for writing out
   }
   cerr  << (decompress?"Dec":"C") << "ompressed " << srcTot <<  " bytes into " << dstTot << " bytes ==> " << (int) ((100*dstTot)/srcTot) << "%" << endl;

   // force wait until all background writes finished
   stopThreads = true;
   for(int swap=0; swap<2; swap++) {
      srcDoneCPU[swap].post();
      dstDoneCPU[swap].post();
   }
   dstDoneIO[0].wait();
   dstDoneIO[1].wait();
   readerThread.join();
   writerThread.join();
}
