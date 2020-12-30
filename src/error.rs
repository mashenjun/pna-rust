
/// My Error Type
#[derive(Debug)]
pub enum KvsError {
    /// MockError is for test usage
    MockError,
}


/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;