import unittest

from scripts.plan_cost_runner import is_measured_join, op_inspect


def scan(rows):
    return {'type': 'TABLE_SCAN', 'intermediate_rows': rows, 'children': []}


def join(join_type, rows, left, right):
    return {
        'type': 'HASH_JOIN',
        'intermediate_rows': rows,
        'extra_info': {'Join Type': join_type},
        'children': [left, right],
    }


class TestPlanCost(unittest.TestCase):
    def test_membership_join_types_are_measured(self):
        for join_type in ('INNER', 'MARK', 'SEMI', 'ANTI', 'RIGHT_SEMI', 'RIGHT_ANTI'):
            with self.subTest(join_type=join_type):
                self.assertTrue(is_measured_join(join(join_type, 5, scan(20), scan(10))))

    def test_membership_rewrite_preserves_input_costs(self):
        inner = join('INNER', 20, scan(100), scan(8))
        mark = join('MARK', 20, inner, scan(10))
        filtered_mark = {'type': 'FILTER', 'intermediate_rows': 5, 'children': [mark]}
        old = op_inspect(filtered_mark)
        for join_type in ('SEMI', 'ANTI'):
            with self.subTest(join_type=join_type):
                new = op_inspect(join(join_type, 5, inner, scan(10)))
                self.assertEqual((old.total, old.build_side, old.probe_side), (40, 18, 120))
                self.assertEqual((new.total, new.build_side, new.probe_side), (25, 18, 120))
                self.assertFalse(new > old)

    def test_more_intermediate_and_build_rows_still_regress(self):
        old = op_inspect(join('MARK', 20, scan(20), scan(10)))
        new = op_inspect(join('SEMI', 30, scan(100), scan(40)))
        self.assertGreater(new, old)


if __name__ == '__main__':
    unittest.main()
