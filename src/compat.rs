//! Avro's real, documented schema resolution rules
//! (<https://avro.apache.org/docs/current/spec.html#Schema+Resolution>):
//! a *reader* schema can read data written by a *writer* schema if every
//! field the reader needs is either present in the writer's data or has
//! a reader-side default, and every shared field's type is either
//! identical or on Avro's documented promotion list.
//!
//! "Backward compatible" (the usual meaning, and Confluent Schema
//! Registry's default check) means: can the *new* schema, acting as
//! reader, read data written by the *old* schema? "Forward compatible"
//! is the reverse — can the *old* schema still read data written by the
//! *new* one, the scenario that matters if producers upgrade before
//! consumers do.

use crate::schema::{AvroType, Field};

#[derive(Debug, Clone, PartialEq)]
pub struct Issue {
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub backward_compatible: bool,
    pub backward_issues: Vec<Issue>,
    pub forward_compatible: bool,
    pub forward_issues: Vec<Issue>,
}

/// Avro's documented numeric/string promotion table: a value written as
/// `writer` can be read by a reader expecting `reader`. Promotion is
/// directional — a `long` writer can be read by a `double` reader, but
/// not the other way around (that would silently truncate).
fn can_promote(writer: &AvroType, reader: &AvroType) -> bool {
    use AvroType::*;
    match (writer, reader) {
        (a, b) if a == b => true,
        (Int, Long) | (Int, Float) | (Int, Double) => true,
        (Long, Float) | (Long, Double) => true,
        (Float, Double) => true,
        (String, Bytes) | (Bytes, String) => true,
        (Nullable(w), Nullable(r)) => can_promote(w, r),
        // A non-nullable writer value is still readable by a nullable
        // reader field of a compatible inner type (the value just never
        // happens to be null) — but not the reverse: a reader expecting
        // a bare (non-nullable) type can't safely assume a nullable
        // writer field is never actually null.
        (w, Nullable(r)) => can_promote(w, r),
        _ => false,
    }
}

fn find<'a>(fields: &'a [Field], name: &str) -> Option<&'a Field> {
    fields.iter().find(|f| f.name == name)
}

/// Computes both compatibility directions between an `old` and `new`
/// version of the same record schema.
pub fn check(old: &[Field], new: &[Field]) -> Report {
    let mut backward_issues = Vec::new();
    let mut forward_issues = Vec::new();

    // New reading old data: every field new adds beyond what old wrote
    // needs a default on the new (reader) side.
    for field in new {
        if find(old, &field.name).is_none() && !field.has_default {
            backward_issues.push(Issue {
                field: field.name.clone(),
                reason: "added without a default — a reader on the new schema has nothing to fill in for data written by the old schema".to_string(),
            });
        }
    }
    // Old reading new data: every field new removed needs a default on
    // the old (reader) side, since new data simply won't carry it.
    for field in old {
        if find(new, &field.name).is_none() && !field.has_default {
            forward_issues.push(Issue {
                field: field.name.clone(),
                reason: "removed in the new schema, and the old schema has no default for it — an old reader has nothing to fill in for data written by the new schema".to_string(),
            });
        }
    }

    // Fields present in both: check type compatibility in each direction.
    for old_field in old {
        let Some(new_field) = find(new, &old_field.name) else {
            continue;
        };
        if !can_promote(&old_field.field_type, &new_field.field_type) {
            backward_issues.push(Issue {
                field: old_field.name.clone(),
                reason: format!(
                    "type changed from {:?} to {:?} — not a valid Avro promotion, a new-schema reader can't read old-schema data for this field",
                    old_field.field_type, new_field.field_type
                ),
            });
        }
        if !can_promote(&new_field.field_type, &old_field.field_type) {
            forward_issues.push(Issue {
                field: old_field.name.clone(),
                reason: format!(
                    "type changed from {:?} to {:?} — not a valid Avro promotion, an old-schema reader can't read new-schema data for this field",
                    old_field.field_type, new_field.field_type
                ),
            });
        }
    }

    Report {
        backward_compatible: backward_issues.is_empty(),
        backward_issues,
        forward_compatible: forward_issues.is_empty(),
        forward_issues,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, ty: AvroType, has_default: bool) -> Field {
        Field {
            name: name.to_string(),
            field_type: ty,
            has_default,
        }
    }

    #[test]
    fn identical_schemas_are_compatible_both_ways() {
        let fields = vec![field("a", AvroType::String, false)];
        let report = check(&fields, &fields);
        assert!(report.backward_compatible);
        assert!(report.forward_compatible);
    }

    #[test]
    fn adding_a_field_with_a_default_is_backward_compatible() {
        let old = vec![field("a", AvroType::String, false)];
        let new = vec![
            field("a", AvroType::String, false),
            field("b", AvroType::Int, true),
        ];
        let report = check(&old, &new);
        assert!(report.backward_compatible, "{:?}", report.backward_issues);
    }

    #[test]
    fn adding_a_field_without_a_default_is_not_backward_compatible() {
        let old = vec![field("a", AvroType::String, false)];
        let new = vec![
            field("a", AvroType::String, false),
            field("b", AvroType::Int, false),
        ];
        let report = check(&old, &new);
        assert!(!report.backward_compatible);
        assert_eq!(report.backward_issues[0].field, "b");
    }

    #[test]
    fn adding_a_field_without_a_default_is_still_forward_compatible() {
        // Old readers never see the new field at all — nothing to resolve.
        let old = vec![field("a", AvroType::String, false)];
        let new = vec![
            field("a", AvroType::String, false),
            field("b", AvroType::Int, false),
        ];
        let report = check(&old, &new);
        assert!(report.forward_compatible);
    }

    #[test]
    fn removing_a_field_that_had_a_default_is_forward_compatible() {
        let old = vec![
            field("a", AvroType::String, false),
            field("b", AvroType::Int, true),
        ];
        let new = vec![field("a", AvroType::String, false)];
        let report = check(&old, &new);
        assert!(report.forward_compatible, "{:?}", report.forward_issues);
    }

    #[test]
    fn removing_a_field_with_no_default_is_not_forward_compatible() {
        let old = vec![
            field("a", AvroType::String, false),
            field("b", AvroType::Int, false),
        ];
        let new = vec![field("a", AvroType::String, false)];
        let report = check(&old, &new);
        assert!(!report.forward_compatible);
        assert_eq!(report.forward_issues[0].field, "b");
    }

    #[test]
    fn removing_a_field_is_always_backward_compatible() {
        let old = vec![
            field("a", AvroType::String, false),
            field("b", AvroType::Int, false),
        ];
        let new = vec![field("a", AvroType::String, false)];
        let report = check(&old, &new);
        assert!(report.backward_compatible);
    }

    #[test]
    fn widening_int_to_long_is_backward_compatible_but_not_forward() {
        // A new Long reader can read old Int-written data (widening on
        // read) — backward compatible. But an old Int reader can't safely
        // read new Long-written data (the value might exceed Int's
        // range) — not forward compatible. Promotion is directional, not
        // a "same or wider, so it's fine both ways" shortcut.
        let old = vec![field("a", AvroType::Int, false)];
        let new = vec![field("a", AvroType::Long, false)];
        let report = check(&old, &new);
        assert!(report.backward_compatible, "{:?}", report.backward_issues);
        assert!(!report.forward_compatible);
    }

    #[test]
    fn narrowing_long_to_int_is_not_backward_compatible_but_is_forward_compatible() {
        // A new Int reader can't read old Long-written data (may not
        // fit) — not backward compatible. But an old Long reader CAN
        // read new Int-written data (Int promotes cleanly to Long) —
        // forward compatible. This is exactly the mirror image of the
        // int->long case above, confirming promotion direction is
        // handled correctly in both call sites, not just one.
        let old = vec![field("a", AvroType::Long, false)];
        let new = vec![field("a", AvroType::Int, false)];
        let report = check(&old, &new);
        assert!(!report.backward_compatible);
        assert!(report.forward_compatible, "{:?}", report.forward_issues);
    }

    #[test]
    fn string_and_bytes_promote_in_both_directions() {
        let old = vec![field("a", AvroType::String, false)];
        let new = vec![field("a", AvroType::Bytes, false)];
        let report = check(&old, &new);
        assert!(report.backward_compatible);
        assert!(report.forward_compatible);
    }

    #[test]
    fn incompatible_type_change_is_flagged_in_both_directions() {
        let old = vec![field("a", AvroType::String, false)];
        let new = vec![field("a", AvroType::Boolean, false)];
        let report = check(&old, &new);
        assert!(!report.backward_compatible);
        assert!(!report.forward_compatible);
        assert!(report.backward_issues[0].reason.contains("String"));
    }

    #[test]
    fn making_a_field_nullable_of_the_same_type_is_compatible() {
        let old = vec![field("a", AvroType::String, false)];
        let new = vec![field(
            "a",
            AvroType::Nullable(Box::new(AvroType::String)),
            false,
        )];
        let report = check(&old, &new);
        assert!(report.backward_compatible);
    }

    #[test]
    fn a_totally_unrelated_schema_reports_multiple_issues() {
        let old = vec![field("a", AvroType::String, false)];
        let new = vec![field("b", AvroType::Int, false)];
        let report = check(&old, &new);
        // "a" removed with no default -> forward issue; "b" added with no
        // default -> backward issue.
        assert_eq!(report.backward_issues.len(), 1);
        assert_eq!(report.forward_issues.len(), 1);
    }
}
