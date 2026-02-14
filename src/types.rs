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
pub enum FileType {
    Heap,
    Sorted,
    Index,
}

