use crate::record::*;
use crate::schema::*;
use crate::types::*;

#[derive(Clone, Debug)]
pub struct Comparison {
    pub operand1: Target,
    pub which_att1: i32,

    pub operand2: Target,
    pub which_att2: i32,

    pub att_type: Type,
    pub op: CompOp,
}

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct OrderMaker {
    pub atts: Vec<(i32, Type)>,
}

pub struct Cnf {
    pub num_ands: i32,
    pub and_list: Vec<Comparison>,
}

impl Cnf {
    pub fn new() -> Self {
        Self {
            num_ands: 0,
            and_list: Vec::new(),
        }
    }

    pub fn get_projections(&self) -> (Vec<i32>, Vec<i32>) {
        let (left, right) = self.get_sort_orders();

        fn map_ordermaker(ordering: OrderMaker) -> Vec<i32> {
            ordering
                .atts
                .iter()
                .map(|att| att.0)
                .collect()
        }

        let left = map_ordermaker(left);
        let right = map_ordermaker(right);

        (left, right)
    }

    pub fn get_sort_orders(&self) -> (OrderMaker, OrderMaker) {
        let mut left = OrderMaker::default();
        let mut right = OrderMaker::default();

        for comparison in &self.and_list {
            let is_join = (comparison.operand1 == Target::Left
                && comparison.operand2 == Target::Right)
                || (comparison.operand2 == Target::Left && comparison.operand1 == Target::Right);

            if is_join {
                if comparison.operand1 == Target::Left {
                    left.atts.push((comparison.which_att1, comparison.att_type));
                } else {
                    right
                        .atts
                        .push((comparison.which_att1, comparison.att_type));
                }

                if comparison.operand2 == Target::Left {
                    left.atts.push((comparison.which_att2, comparison.att_type));
                } else {
                    right
                        .atts
                        .push((comparison.which_att2, comparison.att_type));
                }
            }
        }

        (left, right)
    }

    pub fn run(&self, left: &Record, right: &Record) -> bool {
        self.and_list
            .iter()
            .all(|comparison| comparison.run(left, right))
    }

    pub fn add_comparison(&mut self, comparison: Comparison) {
        self.and_list.push(comparison);
        self.num_ands += 1;
    }
}

impl Default for Comparison {
    fn default() -> Self {
        Comparison {
            operand1: Target::Left,
            which_att1: 0,
            operand2: Target::Right,
            which_att2: 0,
            att_type: Type::Integer,
            op: CompOp::Equal,
        }
    }
}

impl Comparison {
    pub fn run(&self, left: &Record, right: &Record) -> bool {
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
                    MappedAttrData::$attr_type(v) => v,
                    _ => panic!("type mismatch"),
                };
                let right_val = match right_val {
                    MappedAttrData::$attr_type(v) => v,
                    _ => panic!("type mismatch"),
                };

                match self.op {
                    CompOp::Less => left_val < right_val,
                    CompOp::Greater => left_val > right_val,
                    CompOp::Equal => left_val == right_val,
                }
            }};
        }

        match self.att_type {
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

    pub fn run(&self, left: &Record, right: &Record) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        for (att_idx, att_type) in &self.atts {
            let left_data = &left.get_data()[*att_idx as usize];
            let right_data = &right.get_data()[*att_idx as usize];

            let cmp = match att_type {
                Type::Integer => {
                    let left_val = match left_data {
                        MappedAttrData::Integer(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    let right_val = match right_data {
                        MappedAttrData::Integer(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    left_val.cmp(&right_val)
                }
                Type::Float => {
                    let left_val = match left_data {
                        MappedAttrData::Float(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    let right_val = match right_data {
                        MappedAttrData::Float(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    left_val.partial_cmp(&right_val).unwrap_or(Ordering::Equal)
                }
                Type::String => {
                    let left_val = match left_data {
                        MappedAttrData::String(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    let right_val = match right_data {
                        MappedAttrData::String(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    left_val.cmp(right_val)
                }
                _ => panic!("unsupported type for ordering"),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }

        Ordering::Equal
    }

    pub fn run_with_different_order(
        &self,
        left: &Record,
        right: &Record,
        order_right: &OrderMaker,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        let min_len = self.atts.len().min(order_right.atts.len());

        for i in 0..min_len {
            let (left_att_idx, left_att_type) = self.atts[i];
            let (right_att_idx, right_att_type) = order_right.atts[i];

            let left_data = &left.get_data()[left_att_idx as usize];
            let right_data = &right.get_data()[right_att_idx as usize];

            let cmp = match (left_att_type, right_att_type) {
                (Type::Integer, Type::Integer) => {
                    let left_val = match left_data {
                        MappedAttrData::Integer(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    let right_val = match right_data {
                        MappedAttrData::Integer(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    left_val.cmp(&right_val)
                }
                (Type::Float, Type::Float) => {
                    let left_val = match left_data {
                        MappedAttrData::Float(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    let right_val = match right_data {
                        MappedAttrData::Float(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    left_val.partial_cmp(&right_val).unwrap_or(Ordering::Equal)
                }
                (Type::String, Type::String) => {
                    let left_val = match left_data {
                        MappedAttrData::String(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    let right_val = match right_data {
                        MappedAttrData::String(v) => v,
                        _ => panic!("type mismatch"),
                    };
                    left_val.cmp(right_val)
                }
                _ => panic!("type mismatch between left and right attributes"),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }

        Ordering::Equal
    }

    pub fn and_merge(&mut self, other: &OrderMaker) {
        for (att_idx, att_type) in &other.atts {
            if !self
                .atts
                .iter()
                .any(|(existing_idx, _)| existing_idx == att_idx)
            {
                self.atts.push((*att_idx, *att_type));
            }
        }
    }
}
