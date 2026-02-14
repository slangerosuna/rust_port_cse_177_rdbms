use crate::*;

enum Value {
    IntLit(i64),
    FltLit(f64),
    Load(i32),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum OpCode {
    Push,
    ToFlt,
    ToFlt2Down,

    IntNeg,
    IntSub,
    IntAdd,
    IntDiv,
    IntMul,

    FltNeg,
    FltSub,
    FltAdd,
    FltDiv,
    FltMul,
}

pub enum ArithExpr {
    IntLit(i64),
    FltLit(f64),
    Load(String),

    Neg(Box<ArithExpr>),
    Sub(Box<ArithExpr>, Box<ArithExpr>),
    Add(Box<ArithExpr>, Box<ArithExpr>),
    Div(Box<ArithExpr>, Box<ArithExpr>),
    Mul(Box<ArithExpr>, Box<ArithExpr>),
}

impl ArithExpr {
    fn compile(&self, schema: &Schema, ops: &mut Vec<OpCode>, values: &mut Vec<Value>, max_depth: &mut usize, depth: &mut usize) -> Type {
        fn bin_op_match_arm(
            lhs: &Box<ArithExpr>,
            rhs: &Box<ArithExpr>,

            int_op: OpCode,
            flt_op: OpCode,

            schema: &Schema,
            ops: &mut Vec<OpCode>,
            values: &mut Vec<Value>,
            max_depth: &mut usize,
            depth: &mut usize,
        ) -> Type {
            let lhs_type = lhs.compile(schema, ops, values, max_depth, depth);
            let rhs_type = rhs.compile(schema, ops, values, max_depth, depth);

            *depth -= 1;

            match (lhs_type, rhs_type) {
                (Type::Integer, Type::Integer) => {
                    ops.push(int_op);
                    Type::Integer
                }
                (Type::Integer, Type::Float)
                | (Type::Float, Type::Integer)
                | (Type::Float, Type::Float) => {
                    if lhs_type == Type::Integer {
                        ops.push(OpCode::ToFlt2Down);
                    } else if rhs_type == Type::Integer {
                        ops.push(OpCode::ToFlt);
                    }

                    ops.push(flt_op);
                    Type::Float
                }
                _ => panic!(),
            }
        }

        match self {
            ArithExpr::IntLit(i) => {
                *depth += 1;
                if *depth > *max_depth {
                    *max_depth = *depth;
                }

                values.push(Value::IntLit(*i));
                ops.push(OpCode::Push);

                Type::Integer
            }
            ArithExpr::FltLit(f) => {
                *depth += 1;
                if *depth > *max_depth {
                    *max_depth = *depth;
                }

                values.push(Value::FltLit(*f));
                ops.push(OpCode::Push);

                Type::Float
            }
            ArithExpr::Load(name) => {
                *depth += 1;
                if *depth > *max_depth {
                    *max_depth = *depth;
                }

                let index = schema.index_of(&name).unwrap();

                values.push(Value::Load(index as i32));
                let type_ = schema.get_atts()[index].type_;

                ops.push(OpCode::Push);

                type_
            }
            ArithExpr::Neg(parent) => {
                let child_type = parent.compile(schema, ops, values, max_depth, depth);

                ops.push(match child_type {
                    Type::Integer => OpCode::IntNeg,
                    Type::Float => OpCode::FltNeg,
                    _ => panic!(),
                });

                child_type
            }

            ArithExpr::Sub(lhs, rhs) => bin_op_match_arm(lhs, rhs, OpCode::IntSub, OpCode::FltSub, schema, ops, values, max_depth, depth),
            ArithExpr::Add(lhs, rhs) => bin_op_match_arm(lhs, rhs, OpCode::IntAdd, OpCode::FltAdd, schema, ops, values, max_depth, depth),
            ArithExpr::Mul(lhs, rhs) => bin_op_match_arm(lhs, rhs, OpCode::IntMul, OpCode::FltMul, schema, ops, values, max_depth, depth),
            ArithExpr::Div(lhs, rhs) => bin_op_match_arm(lhs, rhs, OpCode::IntDiv, OpCode::FltDiv, schema, ops, values, max_depth, depth),
        }
    }
}

pub struct Function {
    ops: Vec<OpCode>,
    values: Vec<Value>,
    output_type: Type,
    max_depth: usize,
}

impl Function {
    fn new(root_expr: &ArithExpr, schema: &Schema) -> Self {
        let mut ops = Vec::new();
        let mut values = Vec::new();
        let mut max_depth = 0;

        let output_type = root_expr.compile(schema, &mut ops, &mut values, &mut max_depth, &mut 0);

        Function {
            ops,
            values,
            output_type,
            max_depth,
        }
    }

    fn eval(&self, record: &Record) -> MappedAttrData {
        let mut values = self.values.iter().map(|v| unsafe {
            match v {
                Value::IntLit(i) => AttrData { integer: *i },
                Value::FltLit(f) => AttrData { float: *f },
                Value::Load(att_idx) => record.get_raw_attr_data_unchecked(*att_idx as usize),
            }
        });

        let mut stack = Vec::with_capacity(self.max_depth);

        for op in &self.ops {
            macro_rules! bin_op {
                ($float_or_int:ident, $op:tt) => ({
                    let idx = stack.len() - 2;

                    let rhs = stack.pop().unwrap();
                    let lhs = stack[idx];

                    stack[idx] = AttrData {
                        $float_or_int: unsafe { lhs.$float_or_int $op rhs.$float_or_int },
                    };
                });
            }

            match op {
                OpCode::Push => stack.push(values.next().unwrap()),
                OpCode::ToFlt => {
                    let idx = stack.len() - 1;
                    stack[idx] = AttrData {
                        float: unsafe { stack[idx].integer as f64 },
                    }
                },
                OpCode::ToFlt2Down => {
                    let idx = stack.len() - 2;
                    stack[idx] = AttrData {
                        float: unsafe { stack[idx].integer as f64 },
                    }
                },

                OpCode::IntNeg => {
                    let idx = stack.len() - 1;
                    stack[idx] = AttrData {
                        integer: unsafe { -stack[idx].integer },
                    }
                },
                OpCode::FltNeg => {
                    let idx = stack.len() - 1;
                    stack[idx] = AttrData {
                        float: unsafe { -stack[idx].float },
                    }
                },

                OpCode::IntSub => bin_op!(integer, -),
                OpCode::IntAdd => bin_op!(integer, +),
                OpCode::IntDiv => bin_op!(integer, /),
                OpCode::IntMul => bin_op!(integer, *),

                OpCode::FltSub => bin_op!(float, -),
                OpCode::FltAdd => bin_op!(float, +),
                OpCode::FltDiv => bin_op!(float, /),
                OpCode::FltMul => bin_op!(float, *),
            }
        }

        match self.output_type {
            Type::Integer => MappedAttrData::Integer(unsafe { stack[0].integer }),
            Type::Float => MappedAttrData::Float(unsafe { stack[0].float }),
            _ => panic!(),
        }
    }
}
