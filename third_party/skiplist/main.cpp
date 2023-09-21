//
//  main.cpp
//  SkipList
//
//  Created by Paul Ross on 15/11/2015.
//  Copyright (c) 2017 Paul Ross. All rights reserved.
//
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <ctime>

//#include "SkipList.h"
//#include "RollingMedian.h"

#include "test/test_documentation.h"
#include "test/test_functional.h"
#include "test/test_performance.h"
#include "test/test_rolling_median.h"
#include "test/test_concurrent.h"

void test_clock_resolution() {
    int count = 10;
    double average_ticks = 0;
    for (int i = 0; i < count; ++i) {
        clock_t start = clock();
        while (clock() == start) {}
        clock_t diff = clock() - start;
        average_ticks += diff;
    }
    std::cout << "Average ticks (" << count;
    std::cout << " tests) for change in clock(): " << average_ticks / count;
    std::cout << " which is every " << average_ticks / count / CLOCKS_PER_SEC;
    std::cout << " (s)" << std::endl;
    std::cout << "CLOCKS_PER_SEC: " << CLOCKS_PER_SEC << std::endl;
}

#include "SkipList.h"

int test_example_introduction_A(void) {
    int ret = 0;
    // Declare with any type that has sane comparison.
    OrderedStructs::SkipList::HeadNode<double> sl;

    sl.insert(42.0);
    sl.insert(21.0);
    sl.insert(84.0);

    sl.has(42.0); // true
    if (!sl.has(42.0)) {
        ret |= 1;
    }
    sl.size();    // 3
    if (sl.size() != 3) {
        ret |= 2;
    }
    sl.at(1);     // 42.0, throws OrderedStructs::SkipList::IndexError if index out of range
    if (sl.at(1) != 42.0) {
        ret |= 4;
    }
    sl.remove(21.0); // throws OrderedStructs::SkipList::ValueError if value not present

    sl.size();    // 2
    if (sl.size() != 2) {
        ret |= 8;
    }
    sl.at(1);     // 84.0
    if (sl.at(1) != 84.0) {
        ret |= 16;
    }
    return ret;
}

int test_all() {
    int result = 0;

#if 1
    result |= test_example_introduction_A();

    result |= test_functional_all();
    result |= test_rolling_median_all();
    result |= test_documentation_all();
    // Performance tests are very slow if DEBUG as checking
    // integrity is very expensive for large data sets.
#endif
#if 1
#ifndef DEBUG
    result |= test_performance_all();
#endif // DEBUG
#endif
    // test_concurrent_all() only executes performance tests #ifndef DEBUG
    result |= test_concurrent_all();
    return result;
}

int main(int /* argc */, const char *[] /* argv[] */) {
    int result = 0;

    std::cout << "Running skip list tests..." << std::endl;

    time_t start = clock();
    result = test_all();
    double exec = (clock() - start) / (double) CLOCKS_PER_SEC;

//    std::cout << "__cplusplus: \""<< __cplusplus << "\"" << std::endl;
    std::cout << "Final result: ";
    std::cout << (result ? "FAIL" : "PASS") << std::endl;
    std::cout << "Exec time: " << exec << " (s)" << std::endl;
//    test_clock_resolution();
    std::cout << "Bye, bye!\n";
    return 0;
}
