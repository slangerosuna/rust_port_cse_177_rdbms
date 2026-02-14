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
    #[regex("(?i)AND")]
    And,
    #[regex("(?i)GROUP")]
    Group,
    #[regex("(?i)BY")]
    By,
    #[regex("(?i)DISTINCT")]
    Distinct,

    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("<")]
    Lt,
    #[token(">")]
    Gt,
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
