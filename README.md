# avrocheck

Checks two Avro schema versions for backward/forward compatibility
against Avro's own documented schema resolution rules — the same check
Confluent Schema Registry runs before it'll let a Kafka producer register
a new schema version, but as a local CLI you can run in CI before it ever
reaches a registry.

## Usage

```bash
avrocheck old-schema.avsc new-schema.avsc
```

Exit code `1` if either direction is incompatible, `0` if both are clean.

## What "backward" and "forward" mean here

Straight from Avro's own [schema resolution
spec](https://avro.apache.org/docs/current/spec.html#Schema+Resolution):

- **Backward compatible**: a **new**-schema reader can read data a
  writer wrote using the **old** schema. This is what matters when
  consumers upgrade before producers do (the usual, safer rollout order,
  and Confluent's default compatibility mode).
- **Forward compatible**: an **old**-schema reader can still read data a
  writer wrote using the **new** schema. This matters when producers
  upgrade first.

A field added to the new schema needs a default to be backward
compatible (the reader has nothing else to fill in for old data that
never had it). A field removed from the new schema needs a default *on
the old schema* to be forward compatible (an old reader still expects it,
and new data won't provide it). Shared fields whose type changed are
checked against Avro's real promotion table — `int`→`long`→`float`→
`double` widens safely one direction only, and `string`/`bytes` promote
both ways.

## Status: built and verified, including a real promotion-direction mistake caught by my own test

- **24 unit tests** (`cargo test --lib`) across two modules:
  - `schema` (10): primitive type parsing, a `["null", T]` union
    (in either branch order) correctly recognized as a nullable field
    rather than an opaque type, a non-nullable union and a nested
    `record`/`enum` type correctly falling back to opaque (exact-match
    only, not silently misread as something promotable), and a field's
    `"default": null` correctly still counting as *having* a default.
  - `compat` (14): every rule above tested directly — adding a field
    with vs. without a default, removing a field with vs. without a
    default, identical schemas, and the full promotion table.
- **A real mistake in my own first draft, caught by actually running the
  tests, not just writing them**: I first wrote
  `widening_int_to_long_is_compatible_both_ways`, asserting an `int` →
  `long` field change was fine in both directions. It isn't — promotion
  is directional. A new `long`-reader can read old `int`-written data
  fine (backward compatible), but an old `int`-reader can't safely read
  new `long`-written data, since the value might not fit (**not** forward
  compatible). The test failed against my own implementation, which was
  actually correct — the test's *expectation* was wrong, not the code. I
  split it into two correctly-named tests (`..._backward_compatible_but_not_forward`
  and the mirror-image narrowing case) that assert the real, directional
  behavior.
- **Live-verified against three realistic schema pairs and the actual
  compiled binary**: a genuinely fully-compatible change (only a
  defaulted field added, nothing else touched) reported clean on both
  directions with exit code `0`; a change that widened an existing
  field's type from `int` to `long` alongside the safe addition
  correctly reported backward-compatible but forward-**incompatible**
  (a real instance of the exact rule the test above proves, not a
  contrived one — this is what made me go add the fully-clean fixture
  above as a separate case, since the "obviously fine-looking" schema
  change I wrote first wasn't actually clean on both axes); and a schema
  that dropped a required field while adding a new required one reported
  correctly incompatible in *both* directions, with the right field named
  in each direction's issue list.

**Not done / deliberately deferred**: nested `record`/`array`/`map`/
`enum`/`fixed` field types are treated as opaque (exact type shape must
match verbatim; no recursion into a nested record's own fields, no
enum-symbol-set compatibility checking) — genuinely supporting those
would mean re-implementing a meaningful chunk of Avro's full schema
resolution spec rather than the field-level subset this v1 covers, which
is already what a Kafka-topic-value schema (the overwhelmingly common
real case) usually needs; non-`null` unions (`["string", "int"]` with no
`null` branch) are also opaque for the same reason; and schema
namespaces/aliases (Avro's own mechanism for renaming a field without
breaking compatibility) aren't recognized — a rename looks like a
remove-plus-add here.
