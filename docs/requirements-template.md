# Solver Feature Requirements Template

Copy this template into a GitHub feature request issue.

## 1) Solver Use-Case

Describe the exact solver behavior this feature enables.

## 2) Required Inputs

- CGMES profiles: EQ, TP, SV, SSH, other
- Required classes/fields:
- Cardinality assumptions:

## 3) Required Outputs

- Output table: bus, branch, generator, load, other
- Required columns, types, nullability, units:
- Output metadata requirements:

## 4) Validation Rules

List strict rules that must pass for solver correctness.

## 5) Performance Target

- Dataset size expectation:
- Throughput target:
- Max memory target:

## 6) Acceptance Dataset and Tests

- Dataset source (SmallGrid, MiniGrid, custom):
- Minimum acceptance test cases:
- Expected row counts or invariants:

## 7) Backward Compatibility

State whether the change can break existing schema consumers.
