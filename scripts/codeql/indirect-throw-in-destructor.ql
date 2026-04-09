/**
 * @id cpp/duckdb/indirect-throw-in-destructor
 * @name Indirect exception throw reachable from destructor
 * @kind path-problem
 * @problem.severity warning
 */

import cpp

predicate destructorStep(Function pred, Function succ) {
  // direct call from function body
  exists(FunctionCall c |
    c.getEnclosingFunction() = pred and
    c.getTarget() = succ
  )
  or
  // destructor of a member/base destroyed by this destructor
  exists(DestructorDestruction dd, Destructor subd |
    dd.getEnclosingFunction() = pred and
    (
      subd = dd.(DestructorBaseDestruction).getTarget()
      or
      subd = dd.(DestructorFieldDestruction).getExpr().getTarget()
    ) and
    succ = subd
  )
}

query predicate edges(Function pred, Function succ) {
  destructorStep(pred, succ)
}

from Destructor d, Function f, ThrowExpr te
where
  (f = d or destructorStep+(d, f)) and
  te.getEnclosingFunction() = f
select te,
  d,
  f,
  "This throw at $@ is reachable from destructor " + d.getQualifiedName() + ".",
  te,
  te.getFile().getRelativePath() + ":" + te.getLocation().getStartLine().toString()
