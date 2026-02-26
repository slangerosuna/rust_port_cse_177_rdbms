#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Target {
    Left,
    Right,
    Literal,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CompOp {
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Equal,
    NotEqual,
}

impl CompOp {
    pub fn negation(&self) -> Self {
        match self {
            CompOp::Less => CompOp::GreaterEqual,
            CompOp::LessEqual => CompOp::Greater,
            CompOp::Greater => CompOp::LessEqual,
            CompOp::GreaterEqual => CompOp::Less,
            CompOp::Equal => CompOp::NotEqual,
            CompOp::NotEqual => CompOp::Equal,
        }
    }

    pub fn to_normal_form(&self) -> Self {
        match self {
            CompOp::Less => CompOp::Less,
            CompOp::GreaterEqual => CompOp::Less,
            CompOp::Greater => CompOp::Greater,
            CompOp::LessEqual => CompOp::Greater,
            CompOp::Equal => CompOp::Equal,
            CompOp::NotEqual => CompOp::Equal,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FileType {
    Heap,
    Sorted,
    Index,
}
