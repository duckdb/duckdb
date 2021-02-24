/*******************************************************************************
 * ssssort.h++
 *
 * Super Scalar Sample Sort
 * 
 * from ssssort.h (available at https://github.com/lorenzhs/ssssort/blob/b931c024cef3e6d7b7e7fd3ee3e67491d875e021/ssssort.h) 
 * modified (added sort()) by Armin Weiß <armin.weiss@fmi.uni-stuttgart.de>
 *
 *******************************************************************************
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * The MIT License (MIT)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 ******************************************************************************/

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iterator>
#include <random>

namespace ssssort {

/**
 * logBuckets determines how many splitters are used.  Sample Sort partitions
 * the data into buckets, whose number is typically a power of two.  Thus, we
 * specify its base-2 logarithms.  For the partitioning into k buckets, we then
 * need k-1 splitters.  logBuckets is a tuning parameter, typically 7 or 8.
 */
constexpr size_t logBuckets = 8;
constexpr size_t numBuckets = 1 << logBuckets;

/**
 * Type to be used for bucket indices.  In this case, a uint32_t is overkill,
 * but turned out to be fastest.  16-bit arithmetic is peculiarly slow on recent
 * Intel CPUs.  Needs to fit 2*numBuckets-1 (for the step() function), so
 * uint8_t would work for logBuckets = 7
 */
using bucket_t = uint32_t;


// Random number generation engine for sampling.  Declared out-of-class for
// simplicity.  You can swap this out for std::minstd_rand if the Mersenne
// Twister is too slow on your hardware.  It's only minimally slower on mine
// (Haswell i7-4790T).
static std::mt19937 gen{std::random_device{}()};

// Provides different sampling strategies to choose splitters
template <typename Iterator, typename value_type>
struct Sampler {
    // Draw a random sample without replacement using the Fisher-Yates Shuffle.
    // This reorders the input somewhat but the sorting does that anyway.
    static void draw_sample_fisheryates(Iterator begin, Iterator end,
                                        value_type* samples, size_t sample_size)
    {
        // Random generator
        size_t max = end - begin;
        assert(gen.max() >= max);

        for (size_t i = 0; i < sample_size; ++i) {
            size_t index = gen() % max--; // biased, don't care
            std::swap(*(begin + index), *(begin + max));
            samples[i] = *(begin + max);
        }
    }


    // Draw a random sample with replacement by generating random indices. On my
    // machine this results in measurably slower sorting than a
    // Fisher-Yates-based sample, so beware the apparent simplicity.
    static void draw_sample_simplerand(Iterator begin, Iterator end,
                                       value_type* samples, size_t sample_size)
    {
        // Random generator
        size_t size = end - begin;
        assert(gen.max() >= size);

        for (size_t i = 0; i < sample_size; ++i) {
            size_t index = gen() % size; // biased, don't care
            samples[i] = *(begin + index);
        }
    }


    // A completely non-random sample that's beyond terrible on sorted inputs
    static void draw_sample_first(Iterator begin,
                                  __attribute__((unused)) Iterator end,
                                  value_type *samples, size_t sample_size) {
        for (size_t i = 0; i < sample_size; ++i) {
            samples[i] = *(begin + i);
        }
    }

    static void draw_sample(Iterator begin, Iterator end,
                            value_type *samples, size_t sample_size)
    {
        draw_sample_fisheryates(begin, end, samples, sample_size);
    }

};

/**
 * Classify elements into buckets. Template parameter treebits specifies the
 * log2 of the number of buckets (= 1 << treebits).
 */
template <typename InputIterator,  typename OutputIterator, typename value_type,
          size_t treebits = logBuckets>
struct Classifier {
    const size_t num_splitters = (1 << treebits) - 1;
    const size_t splitters_size = 1 << treebits;
    value_type splitters[1 << treebits];

    /// maps items to buckets
    bucket_t* const bktout;
    /// counts bucket sizes
    size_t* const bktsize;

    /**
     * Constructs the splitter tree from the given samples
     */
    Classifier(const value_type *samples, const size_t sample_size,
               bucket_t* const bktout)
        : bktout(bktout)
        , bktsize(new size_t[1 << treebits])
    {
        std::fill(bktsize, bktsize + (1 << treebits), 0);
        build_recursive(samples, samples + sample_size, 1);
    }

    ~Classifier() {
        delete[] bktsize;
    }

    /// recursively builds splitter tree. Used by constructor.
    void build_recursive(const value_type* lo, const value_type* hi, size_t pos) {
        const value_type *mid = lo + (ssize_t)(hi - lo)/2;
        splitters[pos] = *mid;

        if (2 * pos < num_splitters) {
            build_recursive(lo, mid, 2*pos);
            build_recursive(mid + 1, hi , 2*pos + 1);
        }
    }

    /// Push an element down the tree one step. Inlined.
    constexpr bucket_t step(bucket_t i, const value_type &key) const {
        return 2*i + (key > splitters[i]);
    }

    /// Find the bucket for a single element
    constexpr bucket_t find_bucket(const value_type &key) const {
        bucket_t i = 1;
        while (i <= num_splitters) i = step(i, key);
        return (i - splitters_size);
    }

    /**
     * Find the bucket for U elements at the same time. This version will be
     * unrolled by the compiler.  Degree of unrolling is a template parameter, 4
     * is a good choice usually.
     */
    template <int U>
    inline void find_bucket_unroll(InputIterator key, bucket_t* __restrict__ obkt)
    {
        bucket_t i[U];
        for (int u = 0; u < U; ++u) i[u] = 1;

        for (size_t l = 0; l < treebits; ++l) {
            // step on all U keys
            for (int u = 0; u < U; ++u) i[u] = step(i[u], *(key + u));
        }
        for (int u = 0; u < U; ++u) {
            i[u] -= splitters_size;
            obkt[u] = i[u];
            bktsize[i[u]]++;
        }
    }

    /// classify all elements by pushing them down the tree and saving bucket id
    inline void classify(InputIterator begin, InputIterator end,
                         bucket_t* __restrict__ bktout = nullptr)  {
        if (bktout == nullptr) bktout = this->bktout;
        for (InputIterator it = begin; it != end;) {
            bucket_t bucket = find_bucket(*it++);
            *bktout++ = bucket;
            bktsize[bucket]++;
        }
    }

    /// Classify all elements with unrolled bucket finding implementation
    template <int U>
    inline void
    classify_unroll(InputIterator begin, InputIterator end) {
        bucket_t* bktout = this->bktout;
        InputIterator it = begin;
        for (; it + U < end; it += U, bktout += U) {
            find_bucket_unroll<U>(it, bktout);
        }
        // process remainder
        classify(it, end, bktout);
    }

    /**
     * Distribute the elements in [in_begin, in_end) into consecutive buckets,
     * storage for which begins at out_begin.  Need to class classify or
     * classify_unroll before to fill the bktout and bktsize arrays.
     */
    template <int U>
    inline void
    distribute(InputIterator in_begin, InputIterator in_end,
               OutputIterator out_begin)
    {
        // exclusive prefix sum
        for (size_t i = 0, sum = 0; i < numBuckets; ++i) {
            size_t curr_size = bktsize[i];
            bktsize[i] = sum;
            sum += curr_size;
        }
        const size_t n = in_end - in_begin;
        size_t i;
        for (i = 0; i + U < n; i += U) {
            for (int u = 0; u < U; ++u) {
                *(out_begin + bktsize[bktout[i+u]]++) = std::move(*(in_begin + i + u));
            }
        }
        // process the rest
        for (; i < n; ++i) {
            *(out_begin + bktsize[bktout[i]]++) = std::move(*(in_begin + i));
        }
    }

};


// Factor to multiply number of buckets by to obtain the number of samples drawn
inline size_t oversampling_factor(size_t n) {
    double r = std::sqrt(double(n)/(2*numBuckets*(logBuckets+4)));
    	return std::max(static_cast<size_t>(r), static_cast<size_t>(1UL));
}


/**
 * Internal sorter (argument list isn't all that pretty).
 *
 * begin_is_home indicates whether the output should be stored in the range
 * given by begin and end (=true) or out_begin and out_begin + (end - begin)
 * (=false).
 *
 * It is assumed that the range out_begin to out_begin + (end - begin) is valid.
 */
template <typename InputIterator, typename OutputIterator, typename value_type>
void ssssort_int(InputIterator begin, InputIterator end,
                 OutputIterator out_begin,
                 bucket_t* __restrict__ bktout, bool begin_is_home) {
    const size_t n = end - begin;

    // draw and sort sample
    const size_t sample_size = oversampling_factor(n) * numBuckets;
    value_type *samples = new value_type[sample_size];
    Sampler<InputIterator, value_type>::draw_sample(begin, end, samples, sample_size);
    std::sort(samples, samples + sample_size);

    if (samples[0] == samples[sample_size - 1]) {
        // All samples are equal. Clean up and fall back to std::sort
        delete[] samples;
        std::sort(begin, end);
        if (!begin_is_home) {
            std::move(begin, end, out_begin);
        }
        return;
    }

    // classify elements
    Classifier<InputIterator, OutputIterator, value_type, logBuckets>
        classifier(samples, sample_size, bktout);
    delete[] samples;
    classifier.template classify_unroll<6>(begin, end);
    classifier.template distribute<4>(begin, end, out_begin);

    // Recursive calls. offset is the offset into the arrays (/iterators) for
    // the current bucket.
    size_t offset = 0;
    for (size_t i = 0; i < numBuckets; ++i) {
        auto size = classifier.bktsize[i] - offset;
        if (size == 0) continue; // empty bucket
        if (size <= 1024 || (n / size) < 2) {
            // Either it's a small bucket, or very large (more than half of all
            // elements). In either case, we fall back to std::sort.  The reason
            // we're falling back to std::sort in the second case is that the
            // partitioning into buckets is obviously not working (likely
            // because a single value made up the majority of the items in the
            // previous recursion level, but it's also surrounded by lots of
            // other infrequent elements, passing the "all-samples-equal" test.
            std::sort(out_begin + offset, out_begin + classifier.bktsize[i]);
            if (begin_is_home) {
                // uneven recursion level, we have to move the result
                std::move(out_begin + offset,
                          out_begin + classifier.bktsize[i],
                          begin + offset);
            }
        } else {
            // large bucket, apply sample sort recursively
            ssssort_int<OutputIterator, InputIterator, value_type>(
                out_begin + offset,
                out_begin + classifier.bktsize[i], // = out_begin + offset + size
                begin + offset,
                bktout + offset,
                !begin_is_home);
        }
        offset += size;
    }
}

/**
 * Sort [begin, end), output is stored in [out_begin, out_begin + (end-begin))
 *
 * The elements in [begin, end) will be permuted after calling this.
 * Uses <= 2*(end-begin)*sizeof(value_type) bytes of additional memory.
 */
template <typename InputIterator, typename OutputIterator,
          typename value_type = typename std::iterator_traits<InputIterator>::value_type>
void ssssort(InputIterator begin, InputIterator end, OutputIterator out_begin) {
    size_t n = end - begin;
    if (n < 1024) {
        // base case
        std::sort(begin, end);
        std::move(begin, end, out_begin);
        return;
    }

    bucket_t *bktout = new bucket_t[n];
    ssssort_int<InputIterator, OutputIterator, value_type>(begin, end, out_begin, bktout, false);
    delete[] bktout;
}

/**
 * Sort the range [begin, end).
 *
 * Uses <= 3*(end-begin)*sizeof(value_type) bytes of additional memory
 */
template <typename Iterator, typename value_type = typename std::iterator_traits<Iterator>::value_type>
void ssssort(Iterator begin, Iterator end) {
    const size_t n = end - begin;

    if (n < 1024) {
        // base case
        std::sort(begin, end);
        return;
    }

    value_type* out = new value_type[n];
    bucket_t *bktout = new bucket_t[n];
    ssssort_int<Iterator, value_type*, value_type>(begin, end, out, bktout, true);
    delete[] bktout;
    delete[] out;


}
template <typename Iterator, typename value_type = typename std::iterator_traits<Iterator>::value_type, typename Comparator>
void sort(Iterator begin, Iterator end, Comparator less) {
    ssssort(begin, end);
}


}
