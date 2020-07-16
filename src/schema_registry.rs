//! This module contains the code specific for the schema registry.

use crate::schema_registry::SchemaType::{Avro, Json, Other, Protobuf};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use core::fmt;
use curl::easy::{Easy2, Handler, List, WriteError};
use failure::Fail;
use serde::{Deserialize, Serialize};
use serde_json::{json, Error, Map, Value};
use std::fmt::Display;
use std::ops::Deref;
use std::str;

/// By default the schema registry supports three types. It's possible there will be more in the future
/// or to add your own. Therefore the other is one of the schema types.
#[derive(Clone, Debug, PartialEq)]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
    Other(String),
}

/// The schema registry supports sub schema's they will be stored separately in the schema registry
#[derive(Clone, Debug)]
pub struct SuppliedReference {
    pub name: String,
    pub subject: String,
    pub schema: String,
    pub references: Vec<SuppliedReference>,
}

/// Schema as it might be provided to create messages, they will be added to th schema registry if
/// not already present
#[derive(Clone, Debug)]
pub struct SuppliedSchema {
    pub name: Option<String>,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SuppliedReference>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisteredReference {
    pub name: String,
    pub subject: String,
    pub version: u32,
}

/// Schema as retrieved from the schema registry. It's close to the json received and doesn't do
/// type specific transformations.
#[derive(Clone, Debug)]
pub struct RegisteredSchema {
    pub id: u32,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<RegisteredReference>,
}

/// Intermediate result to just handle the byte transformation. When used in a decoder just the
/// id might me enough because the resolved schema is cashed already.
#[derive(Debug)]
pub enum BytesResult {
    Null,
    Invalid(Vec<u8>),
    Valid(u32, Vec<u8>),
}

/// Strategy similar to the one in the Java client. By default schema's needs to be backwards
/// compatible. Historically the only available strategy was the TopicNameStrategy. This meant in
/// practice that a topic could only have one type, or the restriction on backwards compatibility
/// was to be abandoned. Using either of the two other strategies allows multiple types of schema
/// on on topic, while still being able to keep the restriction on schema's being backwards
/// compatible.
/// Depending on the strategy, either the topic, whether the value is used as key, the fully
/// qualified name (only for RecordNameStrategy), or the schema needs to be provided.
#[derive(Clone, Debug)]
pub enum SubjectNameStrategy {
    RecordNameStrategy(String),
    TopicNameStrategy(String, bool),
    TopicRecordNameStrategy(String, String),
    RecordNameStrategyWithSchema(Box<SuppliedSchema>),
    TopicNameStrategyWithSchema(String, bool, Box<SuppliedSchema>),
    TopicRecordNameStrategyWithSchema(String, Box<SuppliedSchema>),
}

/// Just analyses the bytes which are contained in the key or value of an kafka record. When valid
/// it will return the id and the data bytes. The way schema registry messages are encoded is
/// starting with a zero, with the next 4 bytes having the id. The other bytes are the encoded
/// message.
pub fn get_bytes_result(bytes: Option<&[u8]>) -> BytesResult {
    match bytes {
        None => BytesResult::Null,
        Some(p) if p.len() > 4 && p[0] == 0 => {
            let mut buf = &p[1..5];
            let id = buf.read_u32::<BigEndian>().unwrap();
            BytesResult::Valid(id, p[5..].to_owned())
        }
        Some(p) => BytesResult::Invalid(p[..].to_owned()),
    }
}

/// Creates payload that can be included as a key or value on a kafka record
pub fn get_payload(id: u32, encoded_bytes: Vec<u8>) -> Vec<u8> {
    let mut payload = vec![0u8];
    let mut buf = [0u8; 4];
    BigEndian::write_u32(&mut buf, id);
    payload.extend_from_slice(&buf);
    payload.extend_from_slice(encoded_bytes.as_slice());
    payload
}

/// Gets a schema by an id. This is used to get the correct schema te deserialize bytes, with the
/// id that is encoded in the bytes.
pub fn get_schema_by_id(id: u32, schema_registry_url: &str) -> Result<RegisteredSchema, SRCError> {
    let url = format!("{}/schemas/ids/{}", schema_registry_url, id);
    schema_from_url(&url, Option::from(id)).and_then(Ok)
}

pub fn get_schema_by_id_and_type(
    id: u32,
    schema_registry_url: &str,
    schema_type: SchemaType,
) -> Result<RegisteredSchema, SRCError> {
    match get_schema_by_id(id, schema_registry_url) {
        Ok(v) if v.schema_type == schema_type => Ok(v),
        Ok(v) => Err(SRCError::non_retryable_without_cause(&*format!(
            "type {:?}, is not correct",
            v.schema_type
        ))),
        Err(e) => Err(e),
    }
}

/// Gets the schema and the id by supplying a SubjectNameStrategy. This is used to correctly
/// transform a vector to bytes.
pub fn get_schema_by_subject(
    schema_registry_url: &str,
    subject_name_strategy: &SubjectNameStrategy,
) -> Result<RegisteredSchema, SRCError> {
    let subject = get_subject(subject_name_strategy)?;
    match get_schema(subject_name_strategy) {
        None => {
            let url = format!(
                "{}/subjects/{}/versions/latest",
                schema_registry_url, subject
            );
            schema_from_url(&url, None)
        }
        Some(v) => post_schema(&schema_registry_url, subject, v),
    }
}

pub fn get_referenced_schema(
    schema_registry_url: &str,
    registered_reference: &RegisteredReference,
) -> Result<RegisteredSchema, SRCError> {
    let url = format!(
        "{}/subjects/{}/versions/{}",
        schema_registry_url, registered_reference.subject, registered_reference.version
    );
    schema_from_url(&url, None)
}

/// Helper function to get the schema from the strategy.
fn get_schema(subject_name_strategy: &SubjectNameStrategy) -> Option<SuppliedSchema> {
    match subject_name_strategy {
        SubjectNameStrategy::RecordNameStrategy(_) => None,
        SubjectNameStrategy::TopicNameStrategy(_, _) => None,
        SubjectNameStrategy::TopicRecordNameStrategy(_, _) => None,
        SubjectNameStrategy::RecordNameStrategyWithSchema(s) => Some(*s.clone()),
        SubjectNameStrategy::TopicNameStrategyWithSchema(_, _, s) => Some(*s.clone()),
        SubjectNameStrategy::TopicRecordNameStrategyWithSchema(_, s) => Some(*s.clone()),
    }
}

/// Gets the subject part which is also used as key to cache the results. It's constructed so that
/// it's compatible with the Java client.
pub fn get_subject(subject_name_strategy: &SubjectNameStrategy) -> Result<String, SRCError> {
    match subject_name_strategy {
        SubjectNameStrategy::RecordNameStrategy(rn) => Ok(rn.clone()),
        SubjectNameStrategy::TopicNameStrategy(t, is_key) => {
            if *is_key {
                Ok(format!("{}-key", t))
            } else {
                Ok(format!("{}-value", t))
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategy(t, rn) => Ok(format!("{}-{}", t, rn)),
        SubjectNameStrategy::RecordNameStrategyWithSchema(s) => match &s.name {
            None => Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema",
            )),
            Some(n) => Ok(n.clone()),
        },
        SubjectNameStrategy::TopicNameStrategyWithSchema(t, is_key, _) => {
            if *is_key {
                Ok(format!("{}-key", t))
            } else {
                Ok(format!("{}-value", t))
            }
        }
        SubjectNameStrategy::TopicRecordNameStrategyWithSchema(t, s) => match &s.name {
            None => Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema",
            )),
            Some(n) => Ok(format!("{}-{}", t, n)),
        },
    }
}

fn to_registered_reference(reference: &Value) -> Result<RegisteredReference, Error> {
    serde_json::from_value(reference.clone())
}

/// Handles the work of doing an http call and transforming it to a schema while handling
/// possible errors. When there is an error it might be useful to retry.
fn schema_from_url(url: &str, id: Option<u32>) -> Result<RegisteredSchema, SRCError> {
    let easy = match perform_get(url) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::retryable_with_cause(
                e,
                "error performing get to schema registry",
            ))
        }
    };
    let json: Value = to_json(easy)?;
    let id = match id {
        Some(v) => v,
        None => {
            let id_from_response = match json["id"].as_u64() {
                Some(v) => v,
                None => return Err(SRCError::new("Could not get id from response", None, false)),
            };
            id_from_response as u32
        }
    };
    let schema_type = match json["schemaType"].as_str() {
        Some("AVRO") => Avro,
        Some("PROTOBUF") => Protobuf,
        Some("JSON") => Json,
        Some(s) => Other(String::from(s)),
        None => Avro,
    };
    let schema = match json["schema"].as_str() {
        Some(v) => String::from(v),
        None => {
            return Err(SRCError::non_retryable_without_cause(
                "Could not get raw schema from response",
            ))
        }
    };
    let references = match json["references"].as_array() {
        None => vec![],
        Some(v) => match v.iter().map(|j| to_registered_reference(j)).collect() {
            Ok(v) => v,
            Err(e) => {
                return Err(SRCError::non_retryable_with_cause(
                    e,
                    "Error parsing reference",
                ))
            }
        },
    };
    Ok(RegisteredSchema {
        id,
        schema_type,
        schema,
        references,
    })
}

/// Handles posting the schema, and getting back the id. When the schema is already in the schema
/// registry, the matching id is returned. When it's not it depends on the settings of the schema
/// registry. The default config will check if the schema is backwards compatible. One of the ways
/// to do this is to add a default value for new fields.
pub fn post_schema(
    schema_registry_url: &str,
    subject: String,
    schema: SuppliedSchema,
) -> Result<RegisteredSchema, SRCError> {
    let schema_type = match &schema.schema_type {
        Avro => String::from("AVRO"),
        Protobuf => String::from("PROTOBUF"),
        Json => String::from("JSON"),
        Other(v) => v.clone(),
    };
    let references: Vec<RegisteredReference> = match schema
        .references
        .into_iter()
        .map(|r| post_reference(schema_registry_url, &*schema_type, r))
        .collect()
    {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                "Error posting a reference",
            ))
        }
    };
    let url = format!("{}/subjects/{}/versions", schema_registry_url, subject);
    let body = get_body(&*schema_type, &*schema.schema, &*references);
    let id = post_and_get_id(&*url, &*body)?;
    Ok(RegisteredSchema {
        id,
        schema_type: schema.schema_type,
        schema: schema.schema,
        references,
    })
}

fn get_body(schema_type: &str, schema: &str, references: &[RegisteredReference]) -> String {
    let mut root_element = Map::new();
    root_element.insert(String::from("schema"), Value::String(String::from(schema)));
    root_element.insert(
        String::from("schemaType"),
        Value::String(String::from(schema_type)),
    );
    if !references.is_empty() {
        let values: Vec<Value> = references.iter().map(|x| json!(x)).collect();
        root_element.insert(String::from("references"), Value::Array(values));
    }
    let schema_element = Value::Object(root_element);
    schema_element.to_string()
}

fn post_and_get_id(url: &str, body: &str) -> Result<u32, SRCError> {
    let easy = match perform_post(url, body) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::retryable_with_cause(
                e,
                "error performing post to schema registry to get id",
            ))
        }
    };
    let json: Value = to_json(easy)?;
    match json["id"].as_i64() {
        Some(v) => Ok(v as u32),
        None => Err(SRCError::non_retryable_without_cause(
            "Could not get id from response",
        )),
    }
}

fn post_and_get_version(url: &str, body: &str) -> Result<u32, SRCError> {
    let easy = match perform_post(url, body) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::retryable_with_cause(
                e,
                "error performing post to schema registry to get version",
            ))
        }
    };
    let json: Value = to_json(easy)?;
    match json["version"].as_i64() {
        Some(v) => Ok(v as u32),
        None => Err(SRCError::non_retryable_without_cause(
            "Could not get version from response",
        )),
    }
}

fn post_reference(
    schema_registry_url: &str,
    schema_type: &str,
    reference: SuppliedReference,
) -> Result<RegisteredReference, SRCError> {
    let references: Vec<RegisteredReference> = match reference
        .references
        .into_iter()
        .map(|r| post_reference(schema_registry_url, &*schema_type, r))
        .collect()
    {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                "Error posting a reference",
            ))
        }
    };
    let url = format!(
        "{}/subjects/{}/versions",
        schema_registry_url, reference.subject
    );
    let body = get_body(schema_type, &*reference.schema, &*references);
    post_and_get_id(&*url, &*body)?;
    let version_url = format!("{}/subjects/{}", schema_registry_url, reference.subject);
    let version = post_and_get_version(&*version_url, &*body)?;
    Ok(RegisteredReference {
        name: reference.name,
        subject: reference.subject,
        version,
    })
}

/// Does the get, doing it like this makes for more compact code.
fn perform_get(url: &str) -> Result<Easy2<Collector>, curl::Error> {
    let mut easy = Easy2::new(Collector(Vec::new()));
    easy.get(true)?;
    easy.url(url)?;
    easy.perform()?;
    Ok(easy)
}

/// Does the post, setting the headers correctly
fn perform_post(url: &str, body: &str) -> Result<Easy2<Collector>, curl::Error> {
    let mut easy = Easy2::new(Collector(Vec::new()));
    easy.post(true)?;
    easy.url(url)?;
    easy.post_fields_copy(body.as_bytes())?;
    let mut list = List::new();
    list.append("Content-Type: application/vnd.schemaregistry.v1+json")?;
    list.append("Accept: application/vnd.schemaregistry.v1+json")?;
    easy.http_headers(list)?;
    easy.perform()?;
    Ok(easy)
}

/// If the response code was 200, tries to format the payload as json
fn to_json(mut easy: Easy2<Collector>) -> Result<Value, SRCError> {
    match easy.response_code() {
        Ok(200) => (),
        Ok(v) => {
            return Err(SRCError::non_retryable_without_cause(&*format!(
                "Did not get a 200 response code but {} instead",
                v
            )));
        }
        Err(e) => {
            return Err(SRCError::retryable_with_cause(
                e,
                "Encountered error getting http response",
            ));
        }
    }
    let mut data = Vec::new();
    match easy.get_ref() {
        Collector(b) => data.extend_from_slice(b),
    }
    let body = match str::from_utf8(data.as_ref()) {
        Ok(v) => v,
        Err(e) => {
            return Err(SRCError::non_retryable_with_cause(
                e,
                "Invalid UTF-8 sequence",
            ));
        }
    };
    match serde_json::from_str(body) {
        Ok(v) => Ok(v),
        Err(e) => Err(SRCError::non_retryable_with_cause(e, "Invalid json string")),
    }
}

/// Struct to store the payload in
struct Collector(Vec<u8>);

/// Used to easily get the payload from a http call.
impl Handler for Collector {
    fn write(&mut self, data: &[u8]) -> Result<usize, WriteError> {
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
}

/// Error struct which makes it easy to know if the resulting error is also preserved in the cache
/// or not. And whether trying it again might not cause an error.
#[derive(Debug, PartialEq, Fail)]
pub struct SRCError {
    error: String,
    cause: Option<String>,
    retriable: bool,
    cached: bool,
}

/// Implements clone so when an error is returned from the cache, a copy can be returned
impl Clone for SRCError {
    fn clone(&self) -> SRCError {
        let side = match &self.cause {
            Some(v) => Some(String::from(v.deref())),
            None => None,
        };
        SRCError {
            error: String::from(self.error.deref()),
            cause: side,
            retriable: self.retriable,
            cached: self.cached,
        }
    }
}

/// Gives the information from the error in a readable format.
impl fmt::Display for SRCError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.cause {
            Some(cause) => write!(
                f,
                "Error: {}, was cause by {}, it's retriable: {}, it's cached: {}",
                self.error, &cause, self.retriable, self.cached
            ),
            None => write!(
                f,
                "Error: {} had no other cause, it's retriable: {}, it's cached: {}",
                self.error, self.retriable, self.cached
            ),
        }
    }
}

/// Specific error from which can be determined whether retrying might not lead to an error and
/// whether the error is cashed, it's turned into the cashed variant when it's put into the cache.
impl SRCError {
    pub fn new(error: &str, cause: Option<String>, retriable: bool) -> SRCError {
        SRCError {
            error: error.to_owned(),
            cause,
            retriable,
            cached: false,
        }
    }
    pub fn retryable_with_cause<T: Display>(cause: T, error: &str) -> SRCError {
        SRCError::new(error, Some(format!("{}", cause)), true)
    }
    pub fn non_retryable_with_cause<T: Display>(cause: T, error: &str) -> SRCError {
        SRCError::new(error, Some(format!("{}", cause)), false)
    }
    pub fn non_retryable_without_cause(error: &str) -> SRCError {
        SRCError::new(error, None, false)
    }
    /// Should be called before putting the error in the cache
    pub fn into_cache(self) -> SRCError {
        SRCError {
            error: self.error,
            cause: self.cause,
            retriable: self.retriable,
            cached: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema_registry::{
        get_subject, to_json, Collector, SRCError, SchemaType, SubjectNameStrategy, SuppliedSchema,
    };
    use curl::easy::Easy2;

    #[test]
    fn display_record_name_strategy() {
        let sns = SubjectNameStrategy::RecordNameStrategy(String::from("bla"));
        assert_eq!(
            "RecordNameStrategy(\"bla\")".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn display_topic_name_strategy() {
        let sns = SubjectNameStrategy::TopicNameStrategy(String::from("bla"), true);
        assert_eq!(
            "TopicNameStrategy(\"bla\", true)".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn display_topic_record_name_strategy() {
        let sns =
            SubjectNameStrategy::TopicRecordNameStrategy(String::from("bla"), String::from("foo"));
        assert_eq!(
            "TopicRecordNameStrategy(\"bla\", \"foo\")".to_owned(),
            format!("{:?}", sns)
        )
    }

    #[test]
    fn handling_http_error() {
        let easy = Easy2::new(Collector(Vec::new()));
        let result = to_json(easy);
        assert_eq!(
            result,
            Err(SRCError::new(
                "Did not get a 200 response code but 0 instead",
                None,
                false,
            ))
        )
    }

    #[test]
    fn display_error_no_cause() {
        let err = SRCError::new("Could not get id from response", None, false);
        assert_eq!(format!("{}", err), "Error: Could not get id from response had no other cause, it\'s retriable: false, it\'s cached: false".to_owned())
    }

    #[test]
    fn display_error_with_cause() {
        let err = SRCError::new(
            "Could not get id from response",
            Some(String::from("error in response")),
            false,
        );
        assert_eq!(format!("{}", err), "Error: Could not get id from response, was cause by error in response, it\'s retriable: false, it\'s cached: false".to_owned())
    }

    #[test]
    fn error_when_name_mandatory() {
        let strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
            String::from("someTopic"),
            Box::from(SuppliedSchema {
                name: None,
                schema_type: SchemaType::Other(String::from("foo")),
                schema: "".to_string(),
                references: vec![],
            }),
        );

        let result = get_subject(&strategy);

        assert_eq!(
            result,
            Err(SRCError::non_retryable_without_cause(
                "name is mandatory in SuppliedSchema when used in TopicRecordNameStrategyWithSchema"
            ))
        );
    }
}
