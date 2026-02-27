use crate::record::*;
use crate::schema::*;
use crate::types::*;

use std::collections::HashSet;
use std::convert::Into;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Disjunction {
    or_list: Vec<Comparison>,
}

impl Disjunction {
    pub fn new() -> Self {
        Self {
            or_list: Vec::new(),
        }
    }

    pub fn run(&self, left: &Record, right: &Record) -> bool {
        self.or_list
            .iter()
            .any(|comparison| comparison.run(left, right))
    }

    pub fn or(lhs: &Disjunction, rhs: &Disjunction) -> Option<Disjunction> {
        if rhs
            .or_list
            .iter()
            .any(|rhs| lhs.or_list.iter().any(|lhs| lhs.is_negation(rhs)))
        {
            // A + ~A = 1, so the whole disjunction is always true, and we can ignore it in CNF
            return None;
        }

        let or_list = rhs
            .or_list
            .iter()
            .filter(|rhs| !lhs.or_list.iter().any(|lhs| lhs.is_equivalent(rhs)))
            .cloned()
            .chain(lhs.or_list.iter().cloned())
            .collect();

        Some(Disjunction { or_list })
    }

    pub fn negation(self) -> Cnf {
        self.or_list
            .into_iter()
            .map(|mut comparison| {
                comparison.negate();
                comparison
            })
            .fold(Cnf::new(), Cnf::and)
    }
}

impl Into<Cnf> for Comparison {
    fn into(self) -> Cnf {
        let disjunction: Disjunction = self.into();
        disjunction.into()
    }
}

impl Into<Disjunction> for Comparison {
    fn into(self) -> Disjunction {
        Disjunction {
            or_list: vec![self],
        }
    }
}

impl Into<Cnf> for Disjunction {
    fn into(self) -> Cnf {
        let mut cnf = Cnf::new();
        cnf.and_list.push(self);
        cnf
    }
}

use std::ops::{Add, AddAssign, Mul, MulAssign};

macro_rules! impl_add_for_cnf {
    ($lhs:ty) => {
        impl<Rhs: Into<Cnf>> Add<Rhs> for $lhs {
            type Output = Cnf;

            fn add(self, rhs: Rhs) -> Self::Output {
                Cnf::or(self, rhs)
            }
        }
    };
}

impl_add_for_cnf!(Comparison);
impl_add_for_cnf!(Disjunction);
impl_add_for_cnf!(Cnf);

impl<T: Into<Cnf>> AddAssign<T> for Cnf {
    fn add_assign(&mut self, rhs: T) {
        let rhs = rhs.into();
        if rhs.is_false {
            return;
        }

        if self.is_false {
            *self = rhs;
            return;
        }

        self.and_list = self
            .and_list
            .iter()
            .flat_map(|disjunction| {
                rhs.and_list
                    .iter()
                    .filter_map(|rhs_disjunction| Disjunction::or(disjunction, rhs_disjunction))
                    .collect::<Vec<_>>()
            })
            .collect();

        self.minimize();
    }
}

macro_rules! impl_mul_for_cnf {
    ($lhs:ty) => {
        impl<Rhs: Into<Cnf>> Mul<Rhs> for $lhs {
            type Output = Cnf;

            fn mul(self, rhs: Rhs) -> Self::Output {
                Cnf::and(self, rhs)
            }
        }
    };
}

impl_mul_for_cnf!(Comparison);
impl_mul_for_cnf!(Disjunction);
impl_mul_for_cnf!(Cnf);

impl<T: Into<Cnf>> MulAssign<T> for Cnf {
    fn mul_assign(&mut self, rhs: T) {
        let rhs = rhs.into();
        if self.is_false {
            return;
        }
        if rhs.is_false {
            *self = Cnf {
                and_list: Vec::new(),
                is_false: true,
            };
            return;
        }

        self.and_list.extend_from_slice(rhs.and_list.as_slice());

        self.minimize();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cnf {
    pub and_list: Vec<Disjunction>,
    pub is_false: bool,
}

impl Cnf {
    pub fn increase_constants_offset(&mut self, offset: usize) {
        for disjunction in &mut self.and_list {
            for comparison in &mut disjunction.or_list {
                if comparison.operand1 == Target::Literal {
                    comparison.which_att1 += offset as i32;
                }
                if comparison.operand2 == Target::Literal {
                    comparison.which_att2 += offset as i32;
                }
            }
        }
    }

    pub fn minimize(&mut self) {
        // TODO:
    }

    pub fn negation(self) -> Cnf {
        if self.is_false {
            return Cnf::new();
        }

        if self.and_list.is_empty() {
            return Cnf {
                and_list: Vec::new(),
                is_false: true,
            };
        }

        self.and_list
            .into_iter()
            .map(|disjunction| disjunction.negation())
            .fold(Cnf::new(), Cnf::or)
    }

    pub fn or(lhs: impl Into<Cnf>, rhs: impl Into<Cnf>) -> Cnf {
        let mut lhs = lhs.into();
        lhs += rhs;

        lhs
    }

    pub fn and(lhs: impl Into<Cnf>, rhs: impl Into<Cnf>) -> Cnf {
        let mut lhs = lhs.into();
        lhs *= rhs;

        lhs
    }

    pub fn new() -> Self {
        Self {
            and_list: Vec::new(),
            is_false: false,
        }
    }

    pub fn get_projections(&self) -> (Vec<i32>, Vec<i32>) {
        let (left, right) = self.get_sort_orders();

        fn map_ordermaker(ordering: OrderMaker) -> Vec<i32> {
            ordering.atts.iter().map(|att| att.0).collect()
        }

        let left = map_ordermaker(left);
        let right = map_ordermaker(right);

        (left, right)
    }

    pub fn comparisons<'a>(&'a self) -> impl Iterator<Item = &'a Comparison> {
        self.and_list
            .iter()
            .flat_map(|disjunction| disjunction.or_list.iter())
    }

    pub fn has_inequality(&self) -> bool {
        self.comparisons().any(|comparison| {
            comparison.op == CompOp::Less
                || comparison.op == CompOp::Greater
                || comparison.op == CompOp::LessEqual
                || comparison.op == CompOp::GreaterEqual
        })
    }

    pub fn get_sort_orders(&self) -> (OrderMaker, OrderMaker) {
        let mut left = OrderMaker::default();
        let mut right = OrderMaker::default();

        for comparison in self.comparisons() {
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
        if self.is_false {
            return false;
        }

        self.and_list
            .iter()
            .all(|disjunction| disjunction.run(left, right))
    }

    pub fn extract_cnf(left_schema: &Schema, right_schema: &Schema) -> Self {
        let mut cnf = Cnf::new();

        for att in left_schema.get_atts() {
            if right_schema.index_of(&att.name).is_some() {
                let left_att_idx = left_schema.index_of(&att.name).unwrap() as i32;
                let right_att_idx = right_schema.index_of(&att.name).unwrap() as i32;

                let comparison = Comparison {
                    operand1: Target::Left,
                    which_att1: left_att_idx,
                    operand2: Target::Right,
                    which_att2: right_att_idx,
                    att_type: att.type_,
                    op: CompOp::Equal,
                };

                cnf += comparison;
            }
        }

        cnf
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

impl PartialOrd for Comparison {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(Comparison::cmp(&self, other))
    }
}

impl Ord for Comparison {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        Comparison::cmp(&self, other)
    }
}

impl Comparison {
    fn is_negation(&self, other: &Comparison) -> bool {
        match self.op {
            CompOp::Less | CompOp::Greater | CompOp::GreaterEqual | CompOp::LessEqual => {
                (self.op == other.op
                    && self.operand1 == other.operand2
                    && self.which_att1 == other.which_att2
                    && self.operand2 == other.operand1
                    && self.which_att2 == other.which_att1
                    && self.att_type == other.att_type)
                    || (self.op == other.op.negation()
                        && self.operand1 == other.operand1
                        && self.which_att1 == other.which_att1
                        && self.operand2 == other.operand2
                        && self.which_att2 == other.which_att2
                        && self.att_type == other.att_type)
            }
            CompOp::Equal | CompOp::NotEqual => {
                (self.op == CompOp::Equal
                    && other.op == CompOp::NotEqual
                    && self.handles_same_term(other))
                    || (self.op == CompOp::NotEqual
                        && other.op == CompOp::Equal
                        && self.handles_same_term(other))
            }
        }
    }

    fn is_equivalent(&self, other: &Comparison) -> bool {
        self.handles_same_term(other) && !self.is_negation(other)
    }

    fn cmp(lhs: &Self, rhs: &Self) -> std::cmp::Ordering {
        let (lhs, rhs) = (lhs.to_normal_form(), rhs.to_normal_form());

        if lhs.which_att1 != rhs.which_att1 {
            return lhs.which_att1.cmp(&rhs.which_att1);
        }
        if lhs.which_att2 != rhs.which_att2 {
            return lhs.which_att2.cmp(&rhs.which_att2);
        }

        if lhs.operand1 != rhs.operand1 {
            return (lhs.operand1 as u8).cmp(&(rhs.operand1 as u8));
        }
        if lhs.operand2 != rhs.operand2 {
            return (lhs.operand2 as u8).cmp(&(rhs.operand2 as u8));
        }

        if lhs.op != rhs.op {
            return (lhs.op as u8).cmp(&(rhs.op as u8));
        }
        if lhs.att_type != rhs.att_type {
            return (lhs.att_type as u8).cmp(&(rhs.att_type as u8));
        }

        return std::cmp::Ordering::Equal;
    }

    fn to_normal_form(&self) -> Self {
        let should_swap_operands = if self.which_att1 != self.which_att2 {
            self.which_att1 > self.which_att2
        } else {
            (self.operand1 as u8) > (self.operand2 as u8)
        };

        if should_swap_operands {
            Self {
                operand1: self.operand2,
                which_att1: self.which_att2,

                operand2: self.operand1,
                which_att2: self.which_att1,

                att_type: self.att_type,
                op: self.op.to_normal_form(),
            }
        } else {
            Self {
                operand1: self.operand1,
                which_att1: self.which_att1,

                operand2: self.operand2,
                which_att2: self.which_att2,

                att_type: self.att_type,
                op: self.op.to_normal_form(),
            }
        }
    }

    pub fn negate(&mut self) {
        self.op = self.op.negation();
    }

    fn handles_same_term(&self, other: &Comparison) -> bool {
        (self.op == other.op || self.op.negation() == other.op)
            && self.att_type == other.att_type
            && (self.operand1 == other.operand1 && self.which_att1 == other.which_att1
                || self.operand1 == other.operand2 && self.which_att1 == other.which_att2)
            && (self.operand2 == other.operand1 && self.which_att2 == other.which_att1
                || self.operand2 == other.operand2 && self.which_att2 == other.which_att2)
    }

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
                    CompOp::LessEqual => left_val <= right_val,
                    CompOp::Greater => left_val > right_val,
                    CompOp::GreaterEqual => left_val >= right_val,
                    CompOp::Equal => left_val == right_val,
                    CompOp::NotEqual => left_val != right_val,
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
            CompOp::LessEqual => "<=",
            CompOp::Greater => ">",
            CompOp::GreaterEqual => ">=",
            CompOp::Equal => "=",
            CompOp::NotEqual => "!=",
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
