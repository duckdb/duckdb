
import numpy, os, sys
import json

MAX_NODES = 1000

def generate_query_graph(input_file, benchmark, graph_data):
    global current_tone, execution_time, total_nodes
    with open(input_file) as f:
        text = f.read()
        # we only render the first tree, extract it
        text = '{ "result"' + text.split('{ "result"')[1]

    parsed_json = json.loads(text)

    # tones for the different nodes, we use "earth tones"
    tones = [
        "#493829",
        "#816C5B",
        "#A9A18C",
        "#613318",
        "#B99C6B",
        "#A15038",
        "#8F3B1B",
        "#D57500",
        "#DBCA69",
        "#404F24",
        "#668D3C",
        "#BDD09F",
        "#4E6172",
        "#83929F",
        "#A3ADB8"
    ]
    # set fixed tones for the various common operators
    fixed_tone_map = {
        "ORDER_BY": 8,
        "HASH_GROUP_BY": 7,
        "FILTER": 13,
        "SEQ_SCAN": 4,
        "HASH_JOIN": 10,
        "PROJECTION": 5,
        "LIMIT": 0,
        "PIECEWISE_MERGE_JOIN": 1,
        "DISTINCT": 11,
        "DELIM_SCAN": 9
    }
    # remaining (unknown) operators just fetch tones in order
    remaining_tones = []
    for i in range(len(tones)):
        if i not in fixed_tone_map.values():
            remaining_tones.append(i)

    current_tone = 0
    # first parse the total time
    total_time = float(parsed_json["result"])

    # assign an impact factor to nodes
    # this is used for rendering opacity
    execution_time = 0
    node_timings = {}
    node_list = []
    def gather_nodes(node):
        global execution_time
        node_timing = float(node["timing"])
        if node['name'] not in node_timings:
            node_timings[node['name']] = 0
        node_timings[node['name']] += node_timing
        execution_time += node_timing
        node_list.append((node_timing, node))
        children = node["children"]
        for i in range(len(children)):
            gather_nodes(children[i])

    def timing_to_str(timing):
        timing_str = "%.3f" % (timing,)
        if timing_str == "0.000":
            return "0.0"
        # if "e-" in timing_str:
        # 	timing_str = "0.0"
        return timing_str

    def to_percentage(total_time, time):
        fraction = 100 * (time / total_time)
        if fraction < 0.1:
            return "0%"
        else:
            return "%.1f" % (fraction,) + "%"


    # first gather all nodes
    gather_nodes(parsed_json["tree"])

    # get the timing of the different query phases
    meta_info = ""
    if "timings" in parsed_json:
        keys = []
        timings = {}
        for entry in parsed_json["timings"]:
            timings[entry] = parsed_json["timings"][entry]
            keys.append(entry)
        keys.sort()
        meta_info = """<table class="meta-info">
<tr class="phaseheader metainfo">
<th style="font-weight:bold;">Phase</th>
<th style="font-weight:bold;">Time</th>
<th style="font-weight:bold;">Percentage</th>
</tr>
"""
        # add the total time
        meta_info += """<tr class="metainfo mainphase"><th style="font-weight:bold;">Total Time</th><th style="font-weight:bold;">${TOTAL_TIME}s</th><th></th></tr>""".replace("${TOTAL_TIME}", timing_to_str(total_time))
        # add the execution phase
        meta_info += """<tr class="metainfo mainphase"><th>Execution</th><th>${EXECUTION_TIME}s</th><th>${PERCENTAGE}</th></tr>""".replace("${EXECUTION_TIME}", str(execution_time)).replace("${PERCENTAGE}", to_percentage(total_time, execution_time))
        execution_nodes = node_timings.keys()
        execution_nodes = sorted(execution_nodes, key=lambda x: -node_timings[x])
        for node in execution_nodes:
            meta_info += '<tr class="metainfo subphase"><th>${NODE}</th><th>${NODE_TIME}s</th><th>${PERCENTAGE}</th></tr>'.replace("${NODE}", node).replace("${NODE_TIME}", timing_to_str(node_timings[node])).replace("${PERCENTAGE}", to_percentage(total_time, node_timings[node]))


        # add the planning/optimizer phase to the table
        for key in keys:
            row_class = 'subphase' if ">" in key else 'mainphase'
            meta_info += '<tr class="metainfo %s">' % (row_class)
            meta_info += '<th>'
            if ">" in key:
                # subphase
                meta_info += "> " + key.split(" > ")[-1].title().replace("_", " ")
            else:
                # primary phase
                meta_info += key.title().replace("_", " ")
            meta_info += "</th>"
            meta_info += '<th>' + timing_to_str(timings[key]) + "s" + "</th>"
            meta_info += "<th>" + to_percentage(total_time, timings[key]) + "</th>"
            meta_info += "</tr>"
        meta_info += '</table>'

    graph_data['meta_info'][benchmark] = meta_info

    # assign impacts based on % of execution spend in the node
    # <0.1% = 0
    # <1% = 1
    # <5% = 2
    # anything else: 3
    for i in range(len(node_list)):
        percentage = 100 * (node_list[i][0] / total_time)
        if percentage < 0.1:
            node_list[i][1]["impact"] = 0
        elif percentage < 1:
            node_list[i][1]["impact"] = 1
        elif percentage < 5:
            node_list[i][1]["impact"] = 2
        else:
            node_list[i][1]["impact"] = 3


    # now convert the JSON to the format used by treant.js
    def add_spaces_to_big_number(number):
        if len(number) > 4:
            return add_spaces_to_big_number(number[:-3]) + " " + number[-3:]
        else:
            return number

    total_nodes = 0
    def convert_json(node):
        global total_nodes
        total_nodes += 1
        # format is { text { name, title, contact }, HTMLclass, children: [...] }
        global current_tone
        result = {}
        result['text'] = {}
        result['text']['name'] = node["name"] + ' (' + str(node["timing"]) + 's)'
        title = ""
        splits = node["extra_info"].split('\n')
        for i in range(len(splits)):
            if len(splits[i]) > 0:
                title += splits[i].replace('"', '') + "<br>"
        result['text']['title'] = title
        result['text']['contact'] = '[' + add_spaces_to_big_number(str(node["cardinality"])) + ']'
        if node["name"] not in fixed_tone_map:
            fixed_tone_map[node["name"]] = remaining_tones[current_tone]
            current_tone += 1
            if current_tone >= len(remaining_tones):
                current_tone = 0
        if node["impact"] == 0:
            impact_class = "small-impact"
        elif node["impact"] == 1:
            impact_class = "medium-impact"
        elif node["impact"] == 2:
            impact_class = "high-impact"
        else:
            impact_class = "most-impact"
        result['HTMLclass'] = impact_class + ' tone-' + str(fixed_tone_map[node["name"]])
        result['children'] = []
        children = node["children"]
        for i in range(len(children)):
            result['children'].append(convert_json(children[i]))
            if total_nodes > MAX_NODES:
                break
        return result

    new_json = convert_json(parsed_json["tree"])
    graph_data['graph_json'][benchmark] = new_json


def get_benchmark_results(results_folder):
    results = [x for x in os.listdir(results_folder) if x != 'info']
    results = sorted(results)
    results.reverse()
    return results

def get_benchmarks(results_folder):
    info_path = os.path.join(results_folder, 'info')
    # get the group and info
    benchmarks = []
    groups = []
    benchmarks_per_group = {}
    files = os.listdir(info_path)
    files.sort()
    for info_file in files:
        with open(os.path.join(info_path, info_file), 'r') as f:
            header = f.readline()
            splits = header.split(' - ')
            name = splits[0].strip()
            group = splits[1].strip()
            description = f.read()
            if group not in benchmarks_per_group:
                benchmarks_per_group[group] = []
                groups.append(group)
            benchmarks_per_group[group].append(len(benchmarks))
            benchmarks.append([name, description, info_file])
    return (benchmarks, groups, benchmarks_per_group)

def begin_row(f, class_name):
    f.write('<tr class="%s">' % (class_name,))

def end_row(f):
    f.write("</tr>")

def begin_header(f):
    f.write('<th>')

def end_header(f):
    f.write("</th>")

def begin_rotated_header(f):
    f.write('<th class="table-active"><div><div>')

def end_rotated_header(f):
    f.write("</div></div></th>")

def begin_value(f, cl = None):
    if cl:
        f.write('<td class="%s">' % cl)
    else:
        f.write("<td>")

def end_value(f):
    f.write("</td>")

def color_output(output, r, g, b):
    return '<span style="color:rgb(%d,%d,%d);">%s</span>' % (r, g, b, output)

def bold_output(output):
    return "<b>%s</b>" % (output,)

def background_color_output(output, r, g, b):
    return '<div style="background-color:rgb(%d,%d,%d);">%s</div>' % (r, g, b, output)

def write_commit(f, commit):
    f.write('<a href="https://github.com/cwida/duckdb/commit/%s">%s</a>'  % (commit, commit[:4]))

def read_results(file):
    with open(file, 'r') as f:
        lines = f.read().split('\n')
        if len(lines) == 0:
            return ('????', 'Unknown')
        # try to parse everything as numbers
        try:
            numbers = [float(x) for x in lines if len(x) > 0]
            if len(numbers) == 0:
                return ('????', 'Unknown')
            return ("%.2f" % numpy.mean(numbers), 'Result')
        except:
            # failure to parse
            if lines[0] == 'TIMEOUT':
                return ('T', 'Timeout')
            elif lines[0] == 'CRASH':
                return ('C', 'Crash')
            elif lines[0] == 'INCORRECT':
                return ('!', 'Incorrect')
            else:
                return ('????', 'Unknown')

# XXX-results.csv
# contains the benchmark results for run XXX in CSV format:
# XXX-stdout.json
# contains [benchmark_name] -> [stdout]
# XXX-stderr.json
# contains [benchmark_name] -> [stderr]

def pack_benchmarks(benchmark_path, target_path):
    import os
    import json
    name = int(benchmark_path.split('/')[-1].split('-')[0])
    rev = benchmark_path.split('/')[-1].split('-')[1]

    files = os.listdir(benchmark_path)

    result_csv_path = os.path.join(target_path, '_data', 'results', '%06d-results.csv' % (name,))
    stderr_path = os.path.join(target_path, '_includes', 'benchmark_logs', '%06d-stderr.json' % (name,))
    stdout_path = os.path.join(target_path, '_includes', 'benchmark_logs', '%06d-stdout.json' % (name,))
    stderr_html = os.path.join(target_path, 'benchmarks', 'logs', '%06d-stderr.html' % (name,))
    stdout_html = os.path.join(target_path, 'benchmarks', 'logs', '%06d-stdout.html' % (name,))

    result_csv = open(result_csv_path, 'w+')
    stderr = {}
    stdout = {}

    result_csv.write("revision,benchmark,nrun,time\n")

    for f in files:
        with open(os.path.join(benchmark_path, f), 'r') as p:
            contents = p.read()
        splits = f.split('.')
        benchmark_name = splits[0]
        ext = splits[1]
        if ext == 'csv':
            # handle CSV
            count = 0
            error = ""
            for line in contents.split('\n'):
                if len(line) == 0:
                    continue
                try:
                    time = float(line)
                    result_csv.write("%s,%s,%d,%s\n" % (rev, benchmark_name, count, str(time),))
                    count += 1
                except:
                    error = "T"
                    result_csv.write("%s,%s,%d,%s\n" % (rev, benchmark_name, count, error,))
                    break
        if ext == 'stderr':
            stderr[benchmark_name] = contents
        elif ext == 'log':
            stdout[benchmark_name] = contents


    result_csv.close()

    with open(stderr_path, 'w+') as f:
        json.dump(stderr, f)

    with open(stdout_path, 'w+') as f:
        json.dump(stdout, f)

    with open(stderr_html, 'w+') as f:
        f.write("""---
layout: log
title: Revision %d
logfile: /benchmark_logs/%06d-stderr.json
---
""" % (name, name))

    with open(stdout_html, 'w+') as f:
        f.write("""---
layout: log
title: Revision %d
logfile: /benchmark_logs/%06d-stdout.json
---
""" % (name, name))

# result of the packing the info directory:
# one CSV file: benchmark,category

# one JSON file: benchmark-info.json

# one CSV file: category
def pack_info(info_path, target_path):
    import os, json

    name_map = {
        "[tpch-sf1]": "TPC-H",
        "[csv]": "CSV",
        "[tpcds-sf1]": "TPC-DS"
    }
    benchmark_csv_path = os.path.join(target_path, '_data', 'benchmarks.csv')
    categories_csv_path = os.path.join(target_path, '_data', 'categories.csv')
    benchmark_info_json = os.path.join(target_path, '_includes', 'benchmark-info.json')

    result_csv = open(benchmark_csv_path, 'w+')
    result_csv.write("benchmark,category,info\n")

    categories = dict()
    info = dict()

    files = os.listdir(info_path)
    files.sort()
    for fname in files:
        with open(os.path.join(info_path, fname), 'r') as f:
            contents = f.read()
        first_line = contents.split('\n')[0]
        category = first_line.split(' - ')[1]
        categories[category] = 1
        benchmark_name = fname.split('.')[0]
        info[benchmark_name] = contents
        result_csv.write("%s,%s\n" % (benchmark_name,category))

    result_csv.close()

    with open(benchmark_info_json, 'w+') as f:
        json.dump(info, f)


    with open(categories_csv_path, 'w+') as f:
        f.write("category,name\n")
        category_keys = categories.keys()
        category_keys.sort()
        for key in category_keys:
            category_name = key.replace("[", "").replace("]", "").replace("-", " ").title()
            if key in name_map:
                category_name = name_map[key]
            f.write("%s,%s\n" % (key, category_name))



def create_html(results_folder, target_path):
    import json
    # get a list of benchmarks, from most recent to least recent
    results = get_benchmark_results(results_folder)[:50]
    (benchmarks, groups, benchmarks_per_group) = get_benchmarks(results_folder)
    benchmark_results = {}
    has_results = {}
    for benchmark in benchmarks:
        benchmark_results[benchmark[0]] = {}
        has_results[benchmark[0]] = False

    out_dir = os.path.join(target_path, '_includes', 'benchmark_results')
    # pack the benchmark info and write it to the website
    pack_info(os.path.join(results_folder, 'info'), target_path)
    # first gather all the commit results in the dictionary
    for result in results:
        folder = os.path.join(results_folder, result)
        name = int(result.split('-')[0])
        base_stderr = '/benchmarks/logs/%06d-stderr.html' % (name,)
        base_stdout = '/benchmarks/logs/%06d-stdout.html' % (name,)
        base_graph = '/benchmarks/logs/%06d-graph.html' % (name,)

        graph_data = {
            'meta_info': {},
            'graph_json': {}
        }

        # now pack the individual results for this revision and place them on the server
        pack_benchmarks(folder, target_path)
        for benchmark in benchmarks:
            benchmark_name = benchmark[0]
            base_path = os.path.join(folder, benchmark_name)
            benchmark_file = base_path + '.csv'
            log_name = base_path + ".log"
            stderr_name = base_path + ".stderr.log"
            if os.path.isfile(benchmark_file):
                (result_html, result_type) = read_results(benchmark_file)
                has_results[benchmark_name] = True
            else:
                (result_html, result_type) = ('-', 'NoResult')

            result_html = bold_output(result_html)

            extra_info = []
            has_graph = False
            try:
                generate_query_graph(log_name, benchmark_name, graph_data)
                has_graph = True
            except:
                pass
            if has_graph:
                extra_info.append(['Q', base_graph + "?name=" + benchmark_name])
            if os.path.isfile(log_name):
                extra_info.append(['L', base_stdout + "?name=" + benchmark_name])
            if os.path.isfile(stderr_name):
                extra_info.append(['E', base_stderr + "?name=" + benchmark_name])
            if len(extra_info) > 0:
                result_html += " ["
                for i in range(len(extra_info) - 1):
                    result_html += '<a href="%s">%s</a>/' % (extra_info[i][1], extra_info[i][0])
                result_html += '<a href="%s">%s</a>]' % (extra_info[-1][1], extra_info[-1][0])
            table_class = None
            if result_type == 'Crash' or result_type == 'Incorrect':
                table_class = 'table-danger'
            elif result_type == 'Timeout':
                table_class = 'table-warning'
            elif result_type == 'Unknown':
                table_class = 'table-info'

            benchmark_results[benchmark_name][result] = (result_html, table_class)

        # create the graph data for this result
        with open(os.path.join(target_path, '_includes', 'benchmark_logs', '%06d-graph.json' % (name,)), 'w+') as f:
            json.dump(graph_data, f)
        # create the graph html for this result
        with open(os.path.join(target_path, 'benchmarks', 'logs', '%06d-graph.html' % (name,)), 'w+') as f:
            f.write("""---
layout: graph
title: Query Graph for %d
logfile: /benchmark_logs/%06d-graph.json
---""" % (name, name))

    # now write the actual tables
    # we create one table per group
    groups.sort()
    for group in groups:
        fname = group.replace("[", "").replace("]", "").replace("-", "_").replace(" ", "_").lower() + ".html"
        out_html = os.path.join(out_dir, fname)
        with open(out_html, 'w+') as f:
            # the header is all the commits
            begin_row(f, "table-header")
            # one extra header for the benchmark name
            begin_header(f)
            end_header(f)
            for result in results:
                begin_rotated_header(f)
                write_commit(f, result[5:])
                end_rotated_header(f)
            end_row(f)
            #now write the results
            group_elements = sorted(benchmarks_per_group[group], key=lambda x: benchmarks[x][0])
            class_name = "table-even"
            for benchmark_id in group_elements:
                benchmark_name = benchmarks[benchmark_id][0]
                if not has_results[benchmark_name]:
                    # skip benchmarks without results
                    continue
                begin_row(f, class_name)
                # benchmark name
                begin_value(f, 'table-row-header')
                f.write('<a href="/benchmarks/info/info.html?name=%s">%s</a>' % (benchmark_name,benchmark_name))
                end_value(f)
                # benchmark results
                for result in results:
                    (html, table_class) = benchmark_results[benchmark_name][result]
                    begin_value(f, table_class)
                    f.write(html)
                    end_value(f)

                end_row(f)

                if class_name == "table-even":
                    class_name = "table-odd"
                else:
                    class_name = "table-even"

if __name__ == "__main__":
    create_html("benchmark_results", "../duckdb-web")