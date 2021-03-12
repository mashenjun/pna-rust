use crate::error::Error::*;
use crate::{Error, Result, SimpleSerializer};
use core::marker::PhantomData;
use serde::de::{
    DeserializeOwned, DeserializeSeed, EnumAccess, Expected, SeqAccess, VariantAccess, Visitor,
};
use serde::{de, Deserialize};
use std::f32::consts::E;
use std::io::{BufRead, Cursor, Read};

pub struct SimpleDeserializer<R> {
    // This string starts empty and JSON is appended as values are serialized.
    reader: R,
}

impl<R: BufRead> SimpleDeserializer<R> {
    pub fn from_buf_reader(reader: R) -> Self {
        SimpleDeserializer { reader }
    }

    fn next_byte(&mut self) -> Result<u8> {
        let mut bys = [0u8; 1];
        self.reader.read_exact(&mut bys)?;
        Ok(bys[0])
    }

    fn next_line(&mut self) -> Result<String> {
        let mut s = String::new();
        let n = self.reader.read_line(&mut s)?;
        if n == 0 {
            return Err(Eof);
        }

        Ok(s.trim_end().to_string())
    }
}

impl<'de, R> SimpleDeserializer<R> {
    pub fn into_iter<T>(self) -> StreamDeserializer<'de, R, T>
    where
        T: de::Deserialize<'de>,
    {
        // This cannot be an implementation of std::iter::IntoIterator because
        // we need the caller to choose what T is.
        StreamDeserializer {
            deserializer: self,
            failed: false,
            output: PhantomData,
            lifetime: PhantomData,
        }
    }
}

impl<'de, 'a, R: BufRead> de::Deserializer<'de> for &'a mut SimpleDeserializer<R> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        match buf[0] {
            b'+' => self.deserialize_str(visitor), // SimpleString
            // todo
            b'-' => self.deserialize_string(visitor), // Error
            b':' => self.deserialize_i64(visitor),    // Integer
            b'*' => self.deserialize_seq(visitor),    // Array to request
            _ => return Err(Error::NotSupport),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // let mut s = String::new();
        // self.reader.read_line(&mut s)?;
        let s = self.next_line()?;
        match s.parse::<i64>() {
            Ok(x) => visitor.visit_i64(x),
            Err(_) => Err(Error::Syntax),
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // deserialize a single line
        let mut buf = String::new();
        self.reader.read_line(&mut buf)?;
        visitor.visit_str(buf.trim_end())
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let mut buf = String::new();
        self.reader.read_line(&mut buf)?;
        visitor.visit_string(buf.trim_end().to_string())
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(Seq::new(self))
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        return match name {
            "Request" | "Reply" => {
                let value = visitor.visit_enum(Enum::new(self))?;
                Ok(value)
            }
            _ => Err(NotSupport),
        };
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let prefix = self.next_byte()?;
        let cmd = match prefix {
            b'*' => {
                let _args = self.next_line()?;
                let request = self.next_line()?;
                match request.trim_end() {
                    "GET" => "Get",
                    "SET" => "Set",
                    "DEL" => "Remove",
                    _ => {
                        return Err(NotSupport);
                    }
                }
            }
            b'+' => "SingleLine",
            b'-' => "Err",
            b':' => "Int",
            _ => {
                return Err(NotSupport);
            }
        };
        let value = visitor.visit_str(cmd);
        value
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }
}

struct Seq<'a, R: 'a> {
    deserializer: &'a mut SimpleDeserializer<R>,
}

impl<'a, R: 'a> Seq<'a, R> {
    fn new(de: &'a mut SimpleDeserializer<R>) -> Self {
        Self { deserializer: de }
    }
}

impl<'de, 'a, R: BufRead + 'a> SeqAccess<'de> for Seq<'a, R> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.deserializer).map(Some)
    }
}

// a wrapper struct to deal with EnumAccess and VariantAccess
struct Enum<'a, R: 'a> {
    deserializer: &'a mut SimpleDeserializer<R>,
}

impl<'a, R: 'a> Enum<'a, R> {
    fn new(de: &'a mut SimpleDeserializer<R>) -> Self {
        Self { deserializer: de }
    }
}

impl<'de, 'a, R: BufRead + 'a> EnumAccess<'de> for Enum<'a, R> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        // We need to return Variant here, borrow after move will occur if Variant is Self.
        // So we should wapper the deserializer with an new type for EnumAccess and VariantAccess.
        let value = seed.deserialize(
            &mut *self.deserializer, /*re borrow deserializer here */
        )?;
        Ok((value, self))
    }
}

impl<'de, 'a, R: BufRead + 'a> VariantAccess<'de> for Enum<'a, R> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Err(Error::Syntax)
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.deserializer)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::Syntax)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_seq(self.deserializer, visitor)
    }
}

// stream deserialize
pub struct StreamDeserializer<'de, R, T>
where
    T: de::Deserialize<'de>,
{
    deserializer: SimpleDeserializer<R>,
    failed: bool,
    output: PhantomData<T>,
    lifetime: PhantomData<&'de ()>,
}

impl<'de, R, T> StreamDeserializer<'de, R, T>
where
    R: BufRead,
    T: de::Deserialize<'de>,
{
    pub fn new(deserializer: SimpleDeserializer<R>) -> Self {
        StreamDeserializer {
            deserializer: deserializer,
            failed: false,
            output: PhantomData,
            lifetime: PhantomData,
        }
    }
}

impl<'de, R, T> Iterator for StreamDeserializer<'de, R, T>
where
    R: BufRead,
    T: de::Deserialize<'de>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        if self.failed {
            return None;
        }
        let result = T::deserialize(&mut self.deserializer);
        match result {
            Ok(value) => {
                return Some(Ok(value));
            }
            Err(_) => {
                self.failed = true;
                return None;
            }
        }
    }
}

pub fn from_str<T>(s: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut reader = Cursor::new(s);
    from_buf_reader(&mut reader)
}

pub fn from_buf_reader<T, R>(reader: &mut R) -> Result<T>
where
    T: DeserializeOwned,
    R: BufRead,
{
    let mut deserializer = SimpleDeserializer::from_buf_reader(reader);
    let t = T::deserialize(&mut deserializer)?;
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{to_string, Reply, Request};
    use std::io::Read;

    #[test]
    fn test_request_get() {
        let old = Request::Get {
            key: "foo".to_string(),
        };
        let s = to_string(&old).unwrap();
        let new = from_str::<Request>(s.as_str()).unwrap();
        assert_eq!(old, new);
    }

    #[test]
    fn test_request_set() {
        let old = Request::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        let s = to_string(&old).unwrap();
        let new = from_str::<Request>(s.as_str()).unwrap();
        assert_eq!(old, new);
    }

    #[test]
    fn test_request_del() {
        let old = Request::Remove {
            key: "foo".to_string(),
        };
        let s = to_string(&old).unwrap();
        let new = from_str::<Request>(s.as_str()).unwrap();
        assert_eq!(old, new)
    }

    #[test]
    fn test_reply_single_line() {
        let old = Reply::SingleLine("OK".to_string());
        let s = to_string(&old).unwrap();
        let new = from_str::<Reply>(s.as_str()).unwrap();
        assert_eq!(old, new)
    }

    #[test]
    fn test_reply_err() {
        let old = Reply::Err("ERR".to_string());
        let s = to_string(&old).unwrap();
        let new = from_str::<Reply>(s.as_str()).unwrap();
        assert_eq!(old, new)
    }

    #[test]
    fn test_reply_int() {
        let old = Reply::Int(42);
        let s = to_string(&old).unwrap();
        let new = from_str::<Reply>(s.as_str()).unwrap();
        assert_eq!(old, new)
    }
}
