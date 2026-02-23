use crate::*;

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
        on: Condition,
    },
    Scan {
        table_name: String,
    },
}

pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

pub enum SelectAtts {
    Star,
    Atts(Vec<String>),
}

pub struct GroupByAtts {
    pub atts: Vec<String>,
}

pub struct OrderByAtts {
    pub atts: Vec<String>,
}

pub enum ConditionExpr {
    Att(String),
    Literal(String),
    ArithExpr(ArithExpr),
}

pub enum Condition {
    BoolLiteral(bool),
    Comparison(Box<ConditionExpr>, Box<ConditionExpr>, CompOp),

    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
}
