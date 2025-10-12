use crate::record::*;
use crate::schema::*;
use crate::types::*;

#[derive(Clone, Debug)]
pub struct Comparison {
    pub operand1: Target,
    pub which_att1: i32,

    pub operand2: Target,
    pub which_att2: i32,

    pub attr_type: Type,
    pub op: CompOp,
}

#[derive(Default, Clone, Debug)]
pub struct OrderMaker {
    pub atts: Vec<(i32, Type)>,
}

pub struct Cnf {
    pub num_ands: i32,
    pub and_list: Vec<Comparison>,
}

impl Comparison {
    fn run(&self, left: &Record, right: &Record) -> bool {
        let left_val = match self.operand1 {
            Target::Left => &left.get_data()[self.which_att1 as usize],
            Target::Right => &right.get_data()[self.which_att1 as usize],
            Target::Literal => panic!("literal not supported"),
        };

        let right_val = match self.operand2 {
            Target::Left => &left.get_data()[self.which_att2 as usize],
            Target::Right => &right.get_data()[self.which_att2 as usize],
            Target::Literal => panic!("literal not supported"),
        };

        macro_rules! compare {
            ($attr_type:ident) => {{
                let left_val = match left_val {
                    AttrData::$attr_type(v) => v,
                    _ => panic!("type mismatch"),
                };
                let right_val = match right_val {
                    AttrData::$attr_type(v) => v,
                    _ => panic!("type mismatch"),
                };

                match self.op {
                    CompOp::Less => left_val < right_val,
                    CompOp::Greater => left_val > right_val,
                    CompOp::Equal => left_val == right_val,
                }
            }};
        }

        match self.attr_type {
            Type::Integer => compare!(Integer),
            Type::Float => compare!(Float),
            Type::String => compare!(String),
            _ => panic!("can't compare Name type"),
        }
    }
}

impl std::fmt::Display for Comparison {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op_str = match self.op {
            CompOp::Less => "<",
            CompOp::Greater => ">",
            CompOp::Equal => "=",
        };

        write!(
            f,
            "{:?}[{}] {} {:?}[{}] ({:?})",
            self.operand1, self.which_att1, op_str, self.operand2, self.which_att2, self.op
        )
    }
}

impl OrderMaker {
    pub fn new(schema: &Schema) -> Self {
        Self {
            atts: schema
                .get_atts()
                .iter()
                .enumerate()
                .map(|(i, att)| (i as i32, att.type_))
                .collect(),
        }
    }

    pub fn new_projected(schema: &Schema, to_keep: &[usize]) -> Self {
        Self {
            atts: to_keep
                .iter()
                .filter_map(|&att| schema.get_atts().get(att))
                .enumerate()
                .map(|(i, att)| (i as i32, att.type_))
                .collect(),
        }
    }

    pub fn run(&self, left: &Record, right: &Record) -> bool {
        todo!();
    }
}
