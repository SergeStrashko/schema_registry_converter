use crate::async_impl::proto_decoder::ProtoDecoder;
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::async_impl::schema_registry::SrSettings;
use crate::error::SRCError;
use protofish::Value;

pub struct EasyProtoDecoder {
    decoder: Arc<Mutex<ProtoDecoder<'static>>>,
}

impl EasyProtoDecoder {
    pub fn new(sr_settings: SrSettings) -> EasyProtoDecoder {
        let decoder = Arc::new(Mutex::new(ProtoDecoder::new(sr_settings)));
        EasyProtoDecoder { decoder }
    }
    pub async fn decode(&self, bytes: Option<&[u8]>) -> Result<Value, SRCError> {
        let mut lock = self.decoder.lock().await;
        lock.decode(bytes).await
    }
}

#[cfg(test)]
mod tests {
    use mockito::{mock, server_address};
    use test_utils::{get_proto_body, get_proto_hb_schema, get_proto_hb_101};
    use crate::async_impl::schema_registry::SrSettings;
    use crate::async_impl::easy_proto_decoder::EasyProtoDecoder;
    use protofish::Value;

    #[tokio::test]
    async fn test_decoder_default() {
        let _m = mock("GET", "/schemas/ids/7?deleted=true")
            .with_status(200)
            .with_header("content-type", "application/vnd.schemaregistry.v1+json")
            .with_body(&get_proto_body(get_proto_hb_schema(), 1))
            .create();

        let sr_settings = SrSettings::new(format!("http://{}", server_address()));
        let decoder = EasyProtoDecoder::new(sr_settings);
        let heartbeat = decoder.decode(Some(get_proto_hb_101())).await.unwrap();

        let message = match heartbeat {
            Value::Message(x) => *x,
            v => panic!("Other value: {:?} than expected Message", v),
        };

        assert_eq!(Value::UInt64(101u64), message.fields[0].value)
    }
}