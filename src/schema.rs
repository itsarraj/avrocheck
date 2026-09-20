//! Parsing an Avro record schema's `fields` array into something
//! [`crate::compat`] can reason about. Only a `record` schema's top-level
//! fields are modeled — see the README for exactly which type shapes are
//! understood vs. treated as an opaque, exact-match-only type.

use serde_json::Value;

/// The subset of Avro's type system this tool actually reasons about for
/// promotion. Anything else (`record`, `array`, `map`, `enum`, `fixed`,
/// or a named reference to one) is [`AvroType::Opaque`] — compatible only
/// with another opaque of the identical name, never promoted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AvroType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    /// A `["null", T]` (or `[T, "null"]`) union — Avro's idiomatic way of
    /// marking a field nullable/optional. Any other union shape (no
    /// `"null"` branch, or more than two branches) falls back to
    /// [`AvroType::Opaque`] rather than being silently misread.
    Nullable(Box<AvroType>),
    Opaque(String),
}

impl AvroType {
    pub fn parse(value: &Value) -> AvroType {
        match value {
            Value::String(s) => Self::primitive(s),
            Value::Array(branches) => Self::parse_union(branches),
            Value::Object(obj) => obj
                .get("type")
                .and_then(Value::as_str)
                .map(Self::primitive)
                .unwrap_or_else(|| AvroType::Opaque(value.to_string())),
            _ => AvroType::Opaque(value.to_string()),
        }
    }

    fn primitive(name: &str) -> AvroType {
        match name {
            "null" => AvroType::Null,
            "boolean" => AvroType::Boolean,
            "int" => AvroType::Int,
            "long" => AvroType::Long,
            "float" => AvroType::Float,
            "double" => AvroType::Double,
            "bytes" => AvroType::Bytes,
            "string" => AvroType::String,
            other => AvroType::Opaque(other.to_string()),
        }
    }

    fn parse_union(branches: &[Value]) -> AvroType {
        if branches.len() == 2 {
            let parsed: Vec<AvroType> = branches.iter().map(AvroType::parse).collect();
            if parsed[0] == AvroType::Null && parsed[1] != AvroType::Null {
                return AvroType::Nullable(Box::new(parsed[1].clone()));
            }
            if parsed[1] == AvroType::Null && parsed[0] != AvroType::Null {
                return AvroType::Nullable(Box::new(parsed[0].clone()));
            }
        }
        AvroType::Opaque(Value::Array(branches.to_vec()).to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub field_type: AvroType,
    pub has_default: bool,
}

/// Parses a full Avro record schema document's `"fields"` array.
pub fn parse_fields(schema_json: &str) -> Result<Vec<Field>, String> {
    let parsed: Value =
        serde_json::from_str(schema_json).map_err(|e| format!("not valid JSON: {e}"))?;
    let fields = parsed
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| "no \"fields\" array found — is this an Avro record schema?".to_string())?;

    fields
        .iter()
        .map(|f| {
            let name = f
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| "a field is missing its \"name\"".to_string())?
                .to_string();
            let field_type = f
                .get("type")
                .map(AvroType::parse)
                .ok_or_else(|| format!("field {name:?} is missing its \"type\""))?;
            let has_default = f.get("default").is_some();
            Ok(Field {
                name,
                field_type,
                has_default,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_primitive_field_types() {
        let schema = r#"{"type":"record","name":"R","fields":[
            {"name":"a","type":"string"},
            {"name":"b","type":"int"}
        ]}"#;
        let fields = parse_fields(schema).unwrap();
        assert_eq!(fields[0].field_type, AvroType::String);
        assert_eq!(fields[1].field_type, AvroType::Int);
    }

    #[test]
    fn field_with_a_default_is_marked_as_having_one() {
        let schema = r#"{"fields":[{"name":"a","type":"string","default":"x"}]}"#;
        let fields = parse_fields(schema).unwrap();
        assert!(fields[0].has_default);
    }

    #[test]
    fn field_without_a_default_is_marked_accordingly() {
        let schema = r#"{"fields":[{"name":"a","type":"string"}]}"#;
        let fields = parse_fields(schema).unwrap();
        assert!(!fields[0].has_default);
    }

    #[test]
    fn a_null_default_still_counts_as_having_a_default() {
        // Avro allows an explicit `"default": null` for a nullable field —
        // `Value::get` returning `Some(Value::Null)` must still count.
        let schema = r#"{"fields":[{"name":"a","type":["null","string"],"default":null}]}"#;
        let fields = parse_fields(schema).unwrap();
        assert!(fields[0].has_default);
    }

    #[test]
    fn nullable_union_shape_is_recognized_regardless_of_branch_order() {
        let schema = r#"{"fields":[
            {"name":"a","type":["null","string"]},
            {"name":"b","type":["long","null"]}
        ]}"#;
        let fields = parse_fields(schema).unwrap();
        assert_eq!(
            fields[0].field_type,
            AvroType::Nullable(Box::new(AvroType::String))
        );
        assert_eq!(
            fields[1].field_type,
            AvroType::Nullable(Box::new(AvroType::Long))
        );
    }

    #[test]
    fn a_non_nullable_union_shape_is_opaque_not_misread() {
        let schema = r#"{"fields":[{"name":"a","type":["string","int"]}]}"#;
        let fields = parse_fields(schema).unwrap();
        assert!(matches!(fields[0].field_type, AvroType::Opaque(_)));
    }

    #[test]
    fn a_record_or_enum_type_is_opaque() {
        let schema = r#"{"fields":[
            {"name":"a","type":{"type":"enum","name":"Suit","symbols":["A","B"]}}
        ]}"#;
        let fields = parse_fields(schema).unwrap();
        assert!(matches!(fields[0].field_type, AvroType::Opaque(_)));
    }

    #[test]
    fn missing_fields_array_is_a_clean_error() {
        assert!(parse_fields(r#"{"type":"record","name":"R"}"#).is_err());
    }

    #[test]
    fn malformed_json_is_a_clean_error() {
        assert!(parse_fields("not json").is_err());
    }

    #[test]
    fn field_missing_a_name_is_a_clean_error() {
        assert!(parse_fields(r#"{"fields":[{"type":"string"}]}"#).is_err());
    }

    #[test]
    fn field_missing_a_type_is_a_clean_error() {
        assert!(parse_fields(r#"{"fields":[{"name":"a"}]}"#).is_err());
    }
}
