use std::rc::Rc;

#[derive(Debug, Clone)]
pub enum OperandCode {
    Name,
    Integer,
    Float,
    String,
}

#[derive(Debug, Clone)]
pub struct FuncOperand {
    pub code: OperandCode,
    pub value: String,
}

#[derive(Debug, Clone)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Neg, // unary minus
}

#[derive(Debug, Clone)]
pub struct FuncOperator {
    pub left_operand: Option<FuncOperand>,
    pub left_operator: Option<Rc<FuncOperator>>,
    pub op: Option<BinOp>,
    pub right: Option<Rc<FuncOperator>>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct NameList {
    pub name: String,
    pub next: Option<Box<NameList>>,
}

#[derive(Debug, Clone)]
pub struct TableList {
    pub table_name: String,
    pub next: Option<Box<TableList>>,
}

#[derive(Debug, Clone)]
pub enum CompCode {
    LessThan,
    GreaterThan,
    Equals,
}

#[derive(Debug, Clone)]
pub struct ComparisonOp {
    pub code: CompCode,
    pub left: Operand,
    pub right: Operand,
}

#[derive(Debug, Clone)]
pub enum Operand {
    String(String),
    Float(String),
    Integer(String),
    Name(String),
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub cmp: ComparisonOp,
}

#[derive(Debug, Clone)]
pub struct AndList {
    pub left: Condition,
    pub right_and: Option<Box<AndList>>,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub final_function: Option<Rc<FuncOperator>>,
    pub tables: Vec<Table>,
    pub predicate: Option<AndList>,
    pub grouping_atts: Option<Vec<String>>,
    pub atts_to_select: Option<Vec<String>>,
    pub distinct_atts: bool,
}

impl Query {
    pub fn empty() -> Self {
        Query {
            final_function: None,
            tables: vec![],
            predicate: None,
            grouping_atts: None,
            atts_to_select: None,
            distinct_atts: false,
        }
    }
}
