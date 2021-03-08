use std::io::{Read, BufRead, Cursor};
use serde::{de, Deserialize};
use serde::de::{Visitor, DeserializeOwned, EnumAccess, DeserializeSeed, VariantAccess, Expected, SeqAccess};
use crate::{Error, Result, SimpleSerializer};
use crate::error::Error::*;
use std::f32::consts::E;
use core::marker::PhantomData;

pub struct SimpleDeserializer<'de, R: BufRead> {
    // This string starts empty and JSON is appended as values are serialized.
    reader: &'de mut R,
}

impl<'de, R: BufRead> SimpleDeserializer<'de, R> {
    pub fn from_buf_reader(reader: &'de mut R) -> Self {
        SimpleDeserializer { reader }
    }

    fn next_byte(&mut self) -> Result<u8> {
        let mut bys = [0u8;1];
        self.reader.read_exact(&mut bys)?;
        Ok(bys[0])
    }

    fn next_line(&mut self) -> Result<String> {
        let mut s = String::new();
        let n = self.reader.read_line(&mut s)?;
        if n == 0 {
            return Err(Eof)
        }

        Ok(s.trim_end().to_string())
    }

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

impl <'de, R:BufRead> de::Deserializer<'de> for &mut SimpleDeserializer<'de, R> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        match buf[0] {
            b'+' => self.deserialize_str(visitor),      // SimpleString
            // todo
            b'-' => self.deserialize_string(visitor),   // Error
            b':' => self.deserialize_i64(visitor),      // Integer
            b'*' => self.deserialize_seq(visitor),      // Array to request
            _ => return Err(Error::NotSupport),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        // let mut s = String::new();
        // self.reader.read_line(&mut s)?;
        let s = self.next_line()?;
        match s.parse::<i64>() {
            Ok(x) => visitor.visit_i64(x),
            Err(_) => Err(Error::Syntax),
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        // deserialize a single line
        let mut buf = String::new();
        self.reader.read_line(&mut buf)?;
        visitor.visit_str(buf.trim_end())
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        let mut buf = String::new();
        self.reader.read_line(&mut buf)?;
        visitor.visit_string(buf.trim_end().to_string())
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        visitor.visit_seq(Seq::new(&mut *self))
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_struct<V>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        return match name {
            "Request" | "Reply" => {
                let value = visitor.visit_enum(Enum::new(self))?;
                Ok(value)
            },
            _ => { Err(NotSupport) },
        }
    }


    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        let prefix = self.next_byte()?;
        let cmd = match prefix {
            b'*' => {
                let _args = self.next_line()?;
                let request = self.next_line()?;
                match request.trim_end() {
                    "GET" => {
                        "Get"
                    },
                    "SET" => {
                        "Set"
                    },
                    "DEL" => {
                        "Remove"
                    },
                    _ => {return Err(NotSupport);}
                }
            },
            b'+' => {
                "SingleLine"
            },
            b'-' => {
                "Err"
            },
            b':' => {
                "Int"
            }
            _ => {return Err(NotSupport);}
        };
        let value = visitor.visit_str(cmd);
        value
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        unimplemented!()
    }
}

struct Seq<'a, 'de: 'a, R: BufRead> {
    deserializer: &'a mut SimpleDeserializer<'de, R>,
}

impl <'a, 'de, R:BufRead> Seq<'a, 'de, R> {
    fn new(de: &'a mut SimpleDeserializer<'de, R>) -> Self{
        Self{ deserializer: de, }
    }
}

impl<'de, R: BufRead> SeqAccess<'de> for Seq<'_, 'de, R> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>> where
        T: DeserializeSeed<'de> {
        seed.deserialize(&mut *self.deserializer).map(Some)
    }
}

// a wrapper struct to deal with EnumAccess and VariantAccess
struct Enum<'a, 'de: 'a, R: BufRead> {
    deserializer: &'a mut SimpleDeserializer<'de, R>,
}

impl<'a, 'de, R: BufRead> Enum<'a, 'de, R> {
    fn new(de: &'a mut SimpleDeserializer<'de, R>) -> Self {
        Self { deserializer: de }
    }
}

impl<'de, R: BufRead> EnumAccess<'de> for Enum<'_, 'de, R> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)> where
        V: DeserializeSeed<'de> {
        // We need to return Variant here, borrow after move will occur if Variant is Self.
        // So we should wapper the deserializer with an new type for EnumAccess and VariantAccess.
        let value = seed.deserialize(&mut *self.deserializer /*re borrow deserializer here */)?;
        Ok((value, self))
    }
}

impl <'de, R: BufRead> VariantAccess<'de> for Enum<'_, 'de, R> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Err(Error::Syntax)
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value> where
        T: DeserializeSeed<'de> {
        seed.deserialize(self.deserializer)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        Err(Error::Syntax)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value> where
        V: Visitor<'de> {
        de::Deserializer::deserialize_seq(self.deserializer, visitor)
    }
}

// stream deserialize
pub struct StreamDeserializer<'de, R, T>
    where
        R: BufRead,
        T: de::Deserialize<'de>
{
    deserializer: SimpleDeserializer<'de, R>,
    failed: bool,
    output: PhantomData<T>,
    lifetime: PhantomData<&'de ()>,
}

impl<'de, R, T> StreamDeserializer<'de, R, T>
    where
        R: BufRead,
        T: de::Deserialize<'de>,
{
    pub fn new(deserializer: SimpleDeserializer<'de, R>) -> Self {
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
    use crate::{Request,Reply,to_string};

    #[test]
    fn test_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            int: u32,
            seq: Vec<String>,
        }

        let j = r#"{"int":1,"seq":["a","b"]}"#;
        let expected = Test {
            int: 1,
            seq: vec!["a".to_owned(), "b".to_owned()],
        };
        assert_eq!(expected, from_str(j).unwrap());
    }

    #[test]
    fn test_enum() {
        #[derive(Deserialize, PartialEq, Debug)]
        enum E {
            Unit,
            Newtype(u32),
            Tuple(u32, u32),
            Struct { a: u32 },
        }

        let j = r#""Unit""#;
        let expected = E::Unit;
        assert_eq!(expected, from_str(j).unwrap());

        let j = r#"{"Newtype":1}"#;
        let expected = E::Newtype(1);
        assert_eq!(expected, from_str(j).unwrap());

        let j = r#"{"Tuple":[1,2]}"#;
        let expected = E::Tuple(1, 2);
        assert_eq!(expected, from_str(j).unwrap());

        let j = r#"{"Struct":{"a":1}}"#;
        let expected = E::Struct { a: 1 };
        assert_eq!(expected, from_str(j).unwrap());
    }


    #[test]
    fn test_request_get() {
        let old = Request::Get{key:"foo".to_string()};
        let s = to_string(&old).unwrap();
        let new = from_str::<Request>(s.as_str()).unwrap();
        assert_eq!(old, new);
    }

    #[test]
    fn test_request_set() {
        let old = Request::Set{key: "foo".to_string(), value:"bar".to_string()};
        let s = to_string(&old).unwrap();
        let new = from_str::<Request>(s.as_str()).unwrap();
        assert_eq!(old, new);
    }

    #[test]
    fn test_request_del() {
        let old = Request::Remove{key: "foo".to_string()};
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

