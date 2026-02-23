#![allow(unused)]

use logos::Logos;

#[derive(Logos, Debug, PartialEq, Clone)]
pub enum Token {
    #[regex("(?i)SELECT")]
    Select,
    #[regex("(?i)FROM")]
    From,
    #[regex("(?i)WHERE")]
    Where,
    #[regex("(?i)SUM")]
    Sum,
    #[regex("(?i)GROUP")]
    Group,
    #[regex("(?i)ORDER")]
    Order,
    #[regex("(?i)BY")]
    By,
    #[regex("(?i)ASC")]
    Asc,
    #[regex("(?i)DESC")]
    Desc,
    #[regex("(?i)DISTINCT")]
    Distinct,
    #[regex("(?i)LEFT")]
    Left,
    #[regex("(?i)RIGHT")]
    Right,
    #[regex("(?i)FULL")]
    Full,
    #[regex("(?i)INNER")]
    Inner,
    #[regex("(?i)OUTER")]
    Outer,
    #[regex("(?i)JOIN")]
    Join,
    #[regex("(?i)AS")]
    As,
    #[regex("(?i)ON")]
    On,
    #[regex("(?i)AND")]
    And,
    #[regex("(?i)OR")]
    Or,
    #[regex("(?i)NOT")]
    Not,

    // I added a bunch of these tokens for later, they are not currently used in the grammar
    #[regex("(?i)IS")]
    Is,
    #[regex("(?i)NULL")]
    Null,
    #[regex("(?i)IN")]
    In,
    #[regex("(?i)LIKE")]
    Like,
    #[regex("(?i)BETWEEN")]
    Between,
    #[regex("(?i)EXISTS")]
    Exists,
    #[regex("(?i)ALL")]
    All,
    #[regex("(?i)ANY")]
    Any,
    #[regex("(?i)UNION")]
    Union,
    #[regex("(?i)INTERSECT")]
    Intersect,
    #[regex("(?i)EXCEPT")]
    Except,
    #[regex("(?i)UPDATE")]
    Update,
    #[regex("(?i)SET")]
    Set,
    #[regex("(?i)DELETE")]
    Delete,
    #[regex("(?i)INSERT")]
    Insert,
    #[regex("(?i)INTO")]
    r#Into,
    #[regex("(?i)VALUES")]
    Values,
    #[regex("(?i)CREATE")]
    Create,
    #[regex("(?i)DROP")]
    r#Drop,
    #[regex("(?i)TABLE")]
    Table,
    #[regex("(?i)VIEW")]
    View,
    #[regex("(?i)TRUE")]
    True,
    #[regex("(?i)FALSE")]
    False,

    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("<")]
    Lt,
    #[token("<=")]
    Lte,
    #[token(">")]
    Gt,
    #[token(">=")]
    Gte,
    #[token("=")]
    Eq,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("/")]
    Slash,
    #[token("*")]
    Star,
    #[token(",")]
    Comma,

    #[regex(r"-?[0-9]+\.[0-9]*", |lex| lex.slice().to_string())]
    Float(String),

    #[regex(r"-?[0-9]+", |lex| lex.slice().to_string())]
    Integer(String),

    #[regex(r"[A-Za-z][A-Za-z0-9_]*", |lex| lex.slice().to_string())]
    Name(String),

    #[regex(r"'([^'\\\n]|\\.)*'", |lex| {
        let s = lex.slice();
        let inner = &s[1..s.len()-1];

        inner.replace("\\'", "'")
             .replace("\\\\", "\\")
             .replace("\\n", "\n")
             .replace("\\r", "\r")
             .replace("\\0", "\0")
             .replace("\\\"", "\"")
    })]
    Str(String),

    #[regex(r"[ \t\r\n\f]+", logos::skip)]
    Error,
}

pub fn lexer_from_str(s: &str) -> impl Iterator<Item = (usize, Token, usize)> + '_ {
    Token::lexer(s)
        .spanned()
        .filter_map(|(tok, span)| match tok {
            Ok(token) => Some((span.start, token, span.end)),
            Err(_) => None,
        })
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
