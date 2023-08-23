#!/usr/bin/env python3

import header2whatever
from CppHeaderParser.CppHeaderParser import CppMethod


def function_hook(fn: CppMethod, config):
    name = fn['name']
    params = fn['parameters']
    names = ['env'] + [f'param{i}' for i, param in enumerate(params[1:])]
    return_type = fn['rtnType'].split(' ', 1)[1]
    return_type = return_type.rsplit(' ', 1)[0]

    fn.update(
        {
            'name': name,
            'names': ', '.join(names),
            'params': ', '.join(f'{param["type"]} {name}' for param, name in zip(params, names)),
            'return_type': return_type,
            'short_name': (
                '_duckdb_jdbc_' + name.replace('Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1', '').replace('1', '')
            ),
        }
    )


if __name__ == '__main__':
    header2whatever.batch_convert('header2whatever.yaml', 'src/jni', '.')
