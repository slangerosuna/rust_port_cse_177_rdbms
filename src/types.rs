#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Target {
    Left,
    Right,
    Literal,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Integer,
    Float,
    String,
    Name,
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let type_str = match self {
            Type::Integer => "INTEGER",
            Type::Float => "FLOAT",
            Type::String => "STRING",
            _ => "UNKOWN",
        };
        write!(f, "{}", type_str)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CompOp {
    Less,
    Greater,
    Equal,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ArithOp {
    PushInt,
    // I changed it to Flt instead of Dbl in the C++ version because the corresponding
    // `Type` is `Type::Float`, so it feels internally inconsistent to use `Dbl` here.
    PushFlt,
    ToFlt,
    ToFlt2Down,
    IntNeg,
    IntSub,
    IntAdd,
    IntDiv,
    IntMul,
    FltNeg,
    FltSub,
    FltAdd,
    FltDiv,
    FltMul,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FileType {
    Heap,
    Sorted,
    Index,
}

#[derive(Debug)]
pub enum Error {
    General,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::General => write!(f, "General error"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
