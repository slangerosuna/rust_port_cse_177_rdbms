use crate::record::*;
use crate::schema::*;
use crate::types::*;

#[derive(Debug, Clone)]
pub struct ArithmeticOp {
    pub op_type: ArithOp,
    pub record_input: Option<usize>,
    pub literal_value: Option<FunctionValue>,
}

#[derive(Debug, Clone)]
pub enum FunctionValue {
    Integer(i64),
    Float(f64),
}

#[derive(Debug, Clone)]
pub struct Function {
    operations: Vec<ArithmeticOp>,
    returns_int: bool,
}

impl Function {
    pub fn new() -> Self {
        Function {
            operations: Vec::new(),
            returns_int: false,
        }
    }

    pub fn grow_from_parse_tree(
        &mut self,
        parse_tree: &FuncOperator,
        schema: &Schema,
    ) -> Result<()> {
        self.operations.clear();
        let result_type = self.recursively_build(parse_tree, schema)?;
        self.returns_int = matches!(result_type, Type::Integer);
        Ok(())
    }

    fn recursively_build(&mut self, parse_tree: &FuncOperator, schema: &Schema) -> Result<Type> {
        if parse_tree.right.is_none() && parse_tree.left_operand.is_none() && parse_tree.code == '-'
        {
            let operand_type =
                self.recursively_build(parse_tree.left_operator.as_ref().unwrap(), schema)?;

            match operand_type {
                Type::Integer => {
                    self.operations.push(ArithmeticOp {
                        op_type: ArithOp::IntNeg,
                        record_input: None,
                        literal_value: None,
                    });
                    Ok(Type::Integer)
                }
                Type::Float => {
                    self.operations.push(ArithmeticOp {
                        op_type: ArithOp::FltNeg,
                        record_input: None,
                        literal_value: None,
                    });
                    Ok(Type::Float)
                }
                _ => Err(Error::General),
            }
        } else if parse_tree.left_operator.is_none() && parse_tree.right.is_none() {
            let operand = parse_tree.left_operand.as_ref().unwrap();

            match &operand.code {
                NodeType::Name => {
                    let attr_name = &operand.value;
                    if let Some(attr_index) = schema.index_of(attr_name) {
                        let attr_type = schema.find_type(attr_name).unwrap_or(Type::Integer);

                        if matches!(attr_type, Type::String) {
                            return Err(Error::General);
                        }

                        let op_type = match attr_type {
                            Type::Integer => ArithOp::PushInt,
                            Type::Float => ArithOp::PushFlt,
                            _ => return Err(Error::General),
                        };

                        self.operations.push(ArithmeticOp {
                            op_type,
                            record_input: Some(attr_index),
                            literal_value: None,
                        });

                        Ok(attr_type)
                    } else {
                        Err(Error::General)
                    }
                }
                NodeType::Integer => {
                    let value = operand.value.parse::<i64>().map_err(|_| Error::General)?;
                    self.operations.push(ArithmeticOp {
                        op_type: ArithOp::PushInt,
                        record_input: None,
                        literal_value: Some(FunctionValue::Integer(value)),
                    });

                    Ok(Type::Integer)
                }
                NodeType::Float => {
                    let value = operand.value.parse::<f64>().map_err(|_| Error::General)?;
                    self.operations.push(ArithmeticOp {
                        op_type: ArithOp::PushFlt,
                        record_input: None,
                        literal_value: Some(FunctionValue::Float(value)),
                    });

                    Ok(Type::Float)
                }
            }
        } else {
            let left_type =
                self.recursively_build(parse_tree.left_operator.as_ref().unwrap(), schema)?;
            let right_type = self.recursively_build(parse_tree.right.as_ref().unwrap(), schema)?;

            match (left_type, right_type) {
                (Type::Integer, Type::Integer) => {
                    let op_type = match parse_tree.code {
                        '+' => ArithOp::IntAdd,
                        '-' => ArithOp::IntSub,
                        '*' => ArithOp::IntMul,
                        '/' => ArithOp::IntDiv,
                        _ => return Err(Error::General),
                    };

                    self.operations.push(ArithmeticOp {
                        op_type,
                        record_input: None,
                        literal_value: None,
                    });

                    Ok(Type::Integer)
                }
                _ => {
                    if matches!(left_type, Type::Integer) {
                        self.operations.push(ArithmeticOp {
                            op_type: ArithOp::ToFlt2Down,
                            record_input: None,
                            literal_value: None,
                        });
                    }

                    if matches!(right_type, Type::Integer) {
                        self.operations.push(ArithmeticOp {
                            op_type: ArithOp::ToFlt,
                            record_input: None,
                            literal_value: None,
                        });
                    }

                    let op_type = match parse_tree.code {
                        '+' => ArithOp::FltAdd,
                        '-' => ArithOp::FltSub,
                        '*' => ArithOp::FltMul,
                        '/' => ArithOp::FltDiv,
                        _ => return Err(Error::General),
                    };

                    self.operations.push(ArithmeticOp {
                        op_type,
                        record_input: None,
                        literal_value: None,
                    });

                    Ok(Type::Float)
                }
            }
        }
    }

    pub fn apply(&self, record: &Record) -> Result<FunctionValue> {
        let mut stack: Vec<FunctionValue> = Vec::new();

        for op in &self.operations {
            match op.op_type {
                ArithOp::PushInt => {
                    let value = if let Some(record_idx) = op.record_input {
                        if let Some(MappedAttrData::Integer(val)) = record.get_column(record_idx) {
                            *val
                        } else {
                            return Err(Error::General);
                        }
                    } else if let Some(FunctionValue::Integer(val)) = &op.literal_value {
                        *val
                    } else {
                        return Err(Error::General);
                    };
                    stack.push(FunctionValue::Integer(value));
                }
                ArithOp::PushFlt => {
                    let value = if let Some(record_idx) = op.record_input {
                        if let Some(MappedAttrData::Float(val)) = record.get_column(record_idx) {
                            *val
                        } else {
                            return Err(Error::General);
                        }
                    } else if let Some(FunctionValue::Float(val)) = &op.literal_value {
                        *val
                    } else {
                        return Err(Error::General);
                    };
                    stack.push(FunctionValue::Float(value));
                }
                ArithOp::ToFlt => {
                    if let Some(FunctionValue::Integer(val)) = stack.pop() {
                        stack.push(FunctionValue::Float(val as f64));
                    } else {
                        return Err(Error::General);
                    }
                }
                ArithOp::ToFlt2Down => {
                    let len = stack.len();
                    if len >= 2 {
                        if let FunctionValue::Integer(val) = stack[len - 2] {
                            stack[len - 2] = FunctionValue::Float(val as f64);
                        } else {
                            return Err(Error::General);
                        }
                    } else {
                        return Err(Error::General);
                    }
                }
                ArithOp::IntNeg => {
                    if let Some(FunctionValue::Integer(val)) = stack.pop() {
                        stack.push(FunctionValue::Integer(-val));
                    } else {
                        return Err(Error::General);
                    }
                }
                ArithOp::FltNeg => {
                    if let Some(FunctionValue::Float(val)) = stack.pop() {
                        stack.push(FunctionValue::Float(-val));
                    } else {
                        return Err(Error::General);
                    }
                }
                ArithOp::IntAdd | ArithOp::IntSub | ArithOp::IntMul | ArithOp::IntDiv => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        if let (FunctionValue::Integer(l), FunctionValue::Integer(r)) =
                            (left, right)
                        {
                            let result = match op.op_type {
                                ArithOp::IntAdd => l + r,
                                ArithOp::IntSub => l - r,
                                ArithOp::IntMul => l * r,
                                ArithOp::IntDiv => l / r,
                                _ => unreachable!(),
                            };
                            stack.push(FunctionValue::Integer(result));
                        } else {
                            return Err(Error::General);
                        }
                    } else {
                        return Err(Error::General);
                    }
                }
                ArithOp::FltAdd | ArithOp::FltSub | ArithOp::FltMul | ArithOp::FltDiv => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        if let (FunctionValue::Float(l), FunctionValue::Float(r)) = (left, right) {
                            let result = match op.op_type {
                                ArithOp::FltAdd => l + r,
                                ArithOp::FltSub => l - r,
                                ArithOp::FltMul => l * r,
                                ArithOp::FltDiv => l / r,
                                _ => unreachable!(),
                            };
                            stack.push(FunctionValue::Float(result));
                        } else {
                            return Err(Error::General);
                        }
                    } else {
                        return Err(Error::General);
                    }
                }
            }
        }

        if stack.len() == 1 {
            Ok(stack.into_iter().next().unwrap())
        } else {
            Err(Error::General)
        }
    }

    pub fn returns_int(&self) -> bool {
        self.returns_int
    }
}

#[derive(Debug)]
pub struct FuncOperator {
    pub code: char,
    pub left_operator: Option<Box<FuncOperator>>,
    pub right: Option<Box<FuncOperator>>,
    pub left_operand: Option<Operand>,
}

#[derive(Debug)]
pub struct Operand {
    pub code: NodeType,
    pub value: String,
}

#[derive(Debug, PartialEq)]
pub enum NodeType {
    Name,
    Integer,
    Float,
}

impl Default for Function {
    fn default() -> Self {
        Self::new()
    }
}
