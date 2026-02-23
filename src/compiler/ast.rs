use crate::*;

#[derive(Debug)]
pub struct LalrpopError {
    pub message: String,
    pub line: usize,
    pub column: usize,
}

impl std::fmt::Display for LalrpopError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LALRPOP error at line {}, column {}: {}",
            self.line, self.column, self.message
        )
    }
}

#[derive(Debug)]
pub enum Query {
    // SELECT (DISTINCT)? [Atts] FROM [Query] (WHERE [Condition])?
    Select {
        atts: SelectAtts,
        from: Box<Query>,
        r#where: Option<Condition>,
        distinct: bool,
    },
    GroupBy {
        atts: GroupByAtts,
        from: Box<Query>,
    },
    OrderBy {
        asc: bool,
        atts: OrderByAtts,
        from: Box<Query>,
    },
    Join {
        join_type: JoinType,
        left: Box<Query>,
        right: Box<Query>,
        on: Option<Condition>,
    },
    Scan {
        table_names: Vec<String>,
    },
}

#[derive(Debug)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug)]
pub enum SelectAtts {
    Star,
    Atts(Vec<String>),
}

#[derive(Debug)]
pub struct GroupByAtts {
    pub atts: Vec<String>,
}

#[derive(Debug)]
pub struct OrderByAtts {
    pub atts: Vec<String>,
}

#[derive(Debug)]
pub enum ConditionExpr {
    StrLit(String),
    Arith(ArithExpr),
}

#[derive(Debug)]
pub enum Condition {
    BoolLiteral(bool),
    Comparison(Box<ConditionExpr>, Box<ConditionExpr>, CompOp),

    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
}
