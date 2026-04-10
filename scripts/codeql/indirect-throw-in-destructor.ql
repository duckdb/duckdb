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

predicate isRollbackTransaction(Function f) {
  f.getQualifiedName() = "duckdb::DuckTransactionManager::RollbackTransaction"
}

predicate ignoredByRollbackPath(Destructor d, Function f) {
  exists(Function rollback |
    isRollbackTransaction(rollback) and
    destructorStep+(d, rollback) and
    (f = rollback or destructorStep+(rollback, f))
  )
}

predicate isInsideTryCatch(Expr e) {
  exists(TryStmt t |
    e.getParent*() = t
  )
}

predicate callInsideTryCatch(Function pred, Function succ) {
  exists(FunctionCall c |
    c.getEnclosingFunction() = pred and
    c.getTarget() = succ and
    isInsideTryCatch(c)
  )
}

predicate ignoredByTryCatchPath(Destructor d, Function f, ThrowExpr te) {
  isInsideTryCatch(te)
  or
  exists(Function guarded_from, Function guarded_entry |
    (guarded_from = d or destructorStep+(d, guarded_from)) and
    callInsideTryCatch(guarded_from, guarded_entry) and
    (f = guarded_entry or destructorStep+(guarded_entry, f))
  )
}

from Destructor d, Function f, ThrowExpr te
where
  (f = d or destructorStep+(d, f)) and
  te.getEnclosingFunction() = f and
  not ignoredByRollbackPath(d, f) and
  not ignoredByTryCatchPath(d, f, te)
select te,
  d,
  f,
  "This throw at $@ is reachable from destructor " + d.getQualifiedName() + ".",
  te,
  te.getFile().getRelativePath() + ":" + te.getLocation().getStartLine().toString()
