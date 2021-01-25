#pragma once

////////////////////////////////////////////////////////////////////////////////
// tdigest
//
// Copyright (c) 2018 Andrew Werner, All rights reserved.
//
// tdigest is an implementation of Ted Dunning's streaming quantile estimation
// data structure. 
// This implementation is intended to be like the new MergingHistogram.
// It focuses on being in portable C that should be easy to integrate into other
// languages. In particular it provides mechanisms to preallocate all memory 
// at construction time.
//
// The implementation is a direct descendent of 
//  https://github.com/tdunning/t-digest/
//
//
////////////////////////////////////////////////////////////////////////////////

#include <stdlib.h>
#define M_PI 3.14159265358979323846
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <math.h>

template<class T>
struct dig_node_t {
     T mean;
     double count;
};
template<class T> struct td_histogram {
     // compression is a setting used to configure the size of centroids when merged.
     double compression;

     // cap is the total size of nodes
     int cap;
     // merged_nodes is the number of merged nodes at the front of nodes.
     int merged_nodes;
     // unmerged_nodes is the number of buffered nodes.
     int unmerged_nodes;

     double merged_count;
     double unmerged_count;

     dig_node_t<T> nodes[0];
};

// td_init will initialize a td_histogram inside buf which is buf_size bytes.
// If buf_size is too small (smaller than compression + 1) or buf is NULL,
// the returned pointer will be NULL.
//
// In general use td_required_buf_size to figure out what size buffer to
// pass.
template<class T>  static td_histogram<T> *td_init(double compression, size_t buf_size, char *buf) {
     auto *h = (td_histogram<T> *)(buf);
     if (!h) {
          return nullptr;
     }
     bzero((void *)(h), buf_size);
     *h = (td_histogram<T>) {
          .compression = compression,
          .cap = static_cast<int>((buf_size - sizeof(td_histogram<T>)) / sizeof(dig_node_t<T>)),
          .merged_nodes = 0,
          .unmerged_nodes = 0,
          .merged_count = 0,
          .unmerged_count = 0,
	      .nodes{}
     };
     return h;
}

static int cap_from_compression(double compression) {
     return (6 * (int)(compression)) + 10;
}

template<class T> static size_t td_required_buf_size(double compression) {
     return sizeof(td_histogram<T>) +
          (cap_from_compression(compression) * sizeof(dig_node_t<T>));
}


// td_new allocates a new histogram.
// It is similar to init but assumes that it can use malloc.
template<class T>  td_histogram<T> *td_new(double compression){
     size_t memsize = td_required_buf_size<T>(compression);
     return td_init<T>(compression, memsize, (char *)(malloc(memsize)));
}

// td_free frees the memory associated with h.
template<class T>  void td_free(td_histogram<T> *h){
     free((void *)(h));
}

// td_add adds val to h with the specified count.
template<class T>  void td_add(td_histogram<T> *h, T val, double count){
     if (should_merge(h)) {
          merge(h);
     }
     h->nodes[next_node(h)] = (dig_node_t<T>) {
          .mean = val,
          .count = count,
     };
     h->unmerged_nodes++;
     h->unmerged_count += count;
}

// td_merge merges the data from from into into.
template<class T>  void td_merge(td_histogram<T> *into, td_histogram<T> *from){
     merge(into);
     merge(from);
     for (int i = 0; i < from->merged_nodes; i++) {
          dig_node_t<T> *n = &from->nodes[i];
          td_add(into, n->mean, n->count);
     }
}

// td_reset resets a histogram.
template<class T>  void td_reset(td_histogram<T> *h){
     bzero((void *)(&h->nodes[0]), sizeof(dig_node_t<T>)*h->cap);
     h->merged_nodes = 0;
     h->merged_count = 0;
     h->unmerged_nodes = 0;
     h->unmerged_count = 0;
}

static bool is_very_small(double val) {
     return !(val > .000000001 || val < -.000000001);
}


// td_value_at queries h for the value at q.
template<class T> T td_value_at(td_histogram<T> *h, double q){
     merge(h);
     // if left of the first node, use the first node
     // if right of the last node, use the last node, use it
     double goal = q * h->merged_count;
     double k = 0;
     int i = 0;
     dig_node_t<T> *n = nullptr;
     for (i = 0; i < h->merged_nodes; i++) {
          n = &h->nodes[i];
          if (k + n->count > goal) {
               break;
          }
          k += n->count;
     }
     double delta_k = goal - k - (n->count/2);
     if (is_very_small(delta_k)) {
          return n->mean;
     }
     bool right = delta_k > 0;
     if ((right && ((i+1) == h->merged_nodes)) ||
         (!right && (i == 0))) {
          return n->mean;
     }
     dig_node_t<T> *nl;
     dig_node_t<T> *nr;
     if (right) {
          nl = n;
          nr = &h->nodes[i+1];
          k += (nl->count/2);
     } else {
          nl = &h->nodes[i-1];
          nr = n;
          k -= (nl->count/2);
     }
     double x = goal - k;
     // we have two points (0, nl->mean), (nr->count, nr->mean)
     // and we want x
     double m = (nr->mean - nl->mean) / (nl->count/2 + nr->count/2);
     return m * x + nl->mean;
}

// td_value_at queries h for the quantile of val.
// The returned value will be in [0, 1].
template<class T>  double td_quantile_of(td_histogram<T> *h, double val){
     merge(h);
     if (h->merged_nodes == 0) {
          return NAN;
     }
     double k = 0;
     int i = 0;
     dig_node_t<T> *n = nullptr;
     for (i = 0; i < h->merged_nodes; i++) {
          n = &h->nodes[i];
          if (n->mean >= val) {
               break;
          }
          k += n->count;
     }
     if (val == n->mean) {
          // technically this needs to find all of the nodes which contain this value and sum their weight
          double count_at_value = n->count;
          for (i += 1; i < h->merged_nodes && h->nodes[i].mean == n->mean; i++) {
               count_at_value += h->nodes[i].count;
          }
          return (k + (count_at_value/2)) / h->merged_count;
     } else if (val > n->mean) { // past the largest
          return 1;
     } else if (i == 0) {
          return 0;
     }
     // we want to figure out where along the line from the prev node to this node, the value falls
     dig_node_t<T> *nr = n;
     dig_node_t<T> *nl = n-1;
     k -= (nl->count/2);
     // we say that at zero we're at nl->mean
     // and at (nl->count/2 + nr->count/2) we're at nr
     double m = (nr->mean - nl->mean) / (nl->count/2 + nr->count/2);
     double x = (val - nl->mean) / m;
     return (k + x) / h->merged_count;
}

// td_trimmed_mean returns the mean of data from the lo quantile to the
// hi quantile.
template<class T>  double td_trimmed_mean(td_histogram<T> *h, double lo, double hi){
     if (should_merge(h)) {
          merge(h);
     }
     double total_count = h->merged_count;
     double left_tail_count = lo * total_count;
     double right_tail_count = hi * total_count;
     double count_seen = 0;
     double weighted_mean = 0;
     for (int i = 0; i < h->merged_nodes; i++) {
          if (i > 0) {
               count_seen += h->nodes[i-1].count;
          }
          dig_node_t<T> *n = &h->nodes[i];
          if (n->count < left_tail_count) {
               continue;
          }
          if (count_seen > right_tail_count) {
               break;
          }
          double left = count_seen;
          if (left < left_tail_count) {
               left = left_tail_count;
          }
          double right = count_seen + n->count;
          if (right > right_tail_count) {
               right = right_tail_count;
          }
          weighted_mean += n->mean * (right - left);
     }
     double included_count = total_count * (hi - lo);
     return weighted_mean / included_count;
}

// td_total_count returns the total count contained in h.
template<class T>  double td_total_count(td_histogram<T> *h){
     return h->merged_count + h->unmerged_count;
}

// td_total_sum returns the sum of all the data added to h.
template<class T>  double td_total_sum(td_histogram<T> *h){
     dig_node_t<T> *n = nullptr;
     double sum = 0;
     int total_nodes = h->merged_nodes + h->unmerged_nodes;
     for (int i = 0; i < total_nodes; i++) {
          n = &h->nodes[i];
          sum += n->mean * n->count;
     }
     return sum;
}

// td_decay multiplies all countes by factor.
template<class T>  void td_decay(td_histogram<T> *h, double factor) {
	merge(h);
	h->unmerged_count *= factor;
	h->merged_count *= factor;
	for (int i = 0; i < h->merged_nodes; i++) {
		h->nodes[i].count *= factor;
	}
}







template<class T>  static bool should_merge(td_histogram<T> *h) {
     return ((h->merged_nodes + h->unmerged_nodes) == h->cap);
}

template<class T> static int next_node(td_histogram<T> *h) {
     return h->merged_nodes + h->unmerged_nodes;
}
template<class T> static int compare_nodes(const void *v1, const void *v2) {
     auto *n1 = (dig_node_t<T> *)(v1);
     auto *n2 = (dig_node_t<T> *)(v2);
     if (n1->mean < n2->mean) {
          return -1;
     } else if (n1->mean > n2->mean) {
          return 1;
     } else {
          return 0;
     }
}
template<class T> static void merge(td_histogram<T> *h){
     if (h->unmerged_nodes == 0) {
          return;
     }
     int N = h->merged_nodes + h->unmerged_nodes;
     qsort((void *)(h->nodes), N, sizeof(dig_node_t<T>), &compare_nodes<T>);
     double total_count = h->merged_count + h->unmerged_count;
     double denom = 2 * M_PI * total_count * log(total_count);
     double normalizer = h->compression / denom;
     int cur = 0;
     double count_so_far = 0;
     for (int i = 1; i < N; i++) {
          double proposed_count = h->nodes[cur].count + h->nodes[i].count;
          double z = proposed_count * normalizer;
          double q0 = count_so_far / total_count;
          double q2 = (count_so_far + proposed_count) / total_count;
          bool should_add = (z <= (q0 * (1 - q0))) && (z <= (q2 * (1 - q2)));
          if (should_add) {
               h->nodes[cur].count += h->nodes[i].count;
               double delta = h->nodes[i].mean - h->nodes[cur].mean;
               double weighted_delta = (delta * h->nodes[i].count) / h->nodes[cur].count;
               h->nodes[cur].mean += weighted_delta;
          } else {
               count_so_far += h->nodes[cur].count;
               cur++;
               h->nodes[cur] = h->nodes[i];
          }
          if (cur != i) {
               h->nodes[i] = (dig_node_t<T>) {
                    .mean = 0,
                    .count = 0,
               };
          }
     }
     h->merged_nodes = cur+1;
     h->merged_count = total_count;
     h->unmerged_nodes = 0;
     h->unmerged_count = 0;
}





