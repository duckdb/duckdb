import os
from python_helpers import open_utf8


def format_tpch_queries(target_dir, tpch_in, comment):
    with open_utf8(tpch_in, 'r') as f:
        text = f.read()

    for i in range(1, 23):
        qnr = '%02d' % (i,)
        target_file = os.path.join(target_dir, 'q' + qnr + '.benchmark')
        new_text = '''# name: %s
# description: Run query %02d from the TPC-H benchmark (%s)
# group: [sf1]

template %s
QUERY_NUMBER=%d
QUERY_NUMBER_PADDED=%02d''' % (
            target_file,
            i,
            comment,
            tpch_in,
            i,
            i,
        )
        with open_utf8(target_file, 'w+') as f:
            f.write(new_text)


# generate the TPC-H benchmark files
single_threaded_dir = os.path.join('benchmark', 'tpch', 'sf1')
single_threaded_in = os.path.join(single_threaded_dir, 'tpch_sf1.benchmark.in')
format_tpch_queries(single_threaded_dir, single_threaded_in, 'single-threaded')

parallel_threaded_dir = os.path.join('benchmark', 'tpch', 'sf1-parallel')
parallel_threaded_in = os.path.join(parallel_threaded_dir, 'tpch_sf1_parallel.benchmark.in')
format_tpch_queries(parallel_threaded_dir, parallel_threaded_in, '4 threads')
