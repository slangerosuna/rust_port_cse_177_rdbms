use crate::types::*;
use itertools::izip;

// The Clone handles Attribute::Attribute(const Attribute& _other)
#[derive(Clone, Debug)]
pub struct Attribute {
    pub name: String,
    pub type_: Type,
    pub no_distinct: i32,
}

// This handles Attribue::Attribute()
impl Default for Attribute {
    fn default() -> Self {
        Attribute {
            name: String::new(),
            type_: Type::Name,
            no_distinct: 0,
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct Schema {
    no_tuples: i32,
    f_path: String,
    attributes: Vec<Attribute>,
}

impl Schema {
    pub fn new(
        attributes: &[String],
        attribute_types: &[String],
        distincts: &[i32],
        no_tuples: i32,
        f_path: String,
    ) -> Self {
        let attributes = izip!(attributes, attribute_types, distincts)
            .map(|(attr, attr_type, no_distinct)| -> Attribute {
                let type_ = match attr_type.as_str() {
                    "Integer" => Type::Integer,
                    "INTEGER" => Type::Integer,
                    "Float" => Type::Float,
                    "FLOAT" => Type::Float,
                    "String" => Type::String,
                    "STRING" => Type::String,
                    // The C++ implementation didn't have a default case and didn't have a case for
                    // Type::Name, not sure what's up with that.
                    _ => panic!(),
                };

                Attribute {
                    name: attr.clone(),
                    type_,
                    no_distinct: *no_distinct,
                }
            })
            .collect::<Vec<_>>();

        Schema {
            no_tuples,
            f_path: f_path,
            attributes,
        }
    }

    pub fn new_no_attributes(no_tuples: i32, f_path: String) -> Self {
        Self::new(&[], &[], &[], no_tuples, f_path)
    }

    pub fn from_attributes(
        attributes: &[String],
        attribute_types: &[String],
        distincts: &[i32],
    ) -> Self {
        Self::new(attributes, attribute_types, distincts, 0, "".to_string())
    }

    pub fn get_num_atts(&self) -> usize {
        self.attributes.len()
    }

    // In the C++ version this is GetNumAtts, but get_atts_len feels like the more idiomatice name
    pub fn get_atts_len(&self) -> usize {
        self.attributes.len()
    }

    pub fn get_atts(&self) -> &Vec<Attribute> {
        &self.attributes
    }

    pub fn get_no_tuples(&self) -> i32 {
        self.no_tuples
    }

    pub fn get_f_path(&self) -> &str {
        &self.f_path
    }

    pub fn set_no_tuples(&mut self, no_tuples: i32) {
        self.no_tuples = no_tuples;
    }

    pub fn set_f_path(&mut self, f_path: &str) {
        self.f_path = f_path.to_string();
    }

    pub fn append(&mut self, other: &Schema) -> bool {
        if other
            .attributes
            .iter()
            .any(|attr| self.index_of(&attr.name).is_some())
        {
            return false;
        }
        self.attributes.extend_from_slice(&other.attributes);
        true
    }

    pub fn join_right(&mut self, other: &Schema) {
        let (other_attributes, join_attributes) = other
            .attributes
            .iter()
            .partition::<Vec<_>, _>(|attr| self.index_of(&attr.name).is_none());

        for attr in other_attributes {
            self.attributes.push(attr.clone());
        }

        let mut no_tuples = self.no_tuples * other.no_tuples;

        for attr in join_attributes {
            let index = self.index_of(&attr.name).unwrap();
            let self_distincts = self.attributes[index].no_distinct as f64;

            let index = other.index_of(&attr.name).unwrap();
            let other_distincts = other.attributes[index].no_distinct as f64;

            let max_distincts = f64::max(self_distincts, other_distincts);

            if max_distincts != 0.0 {
                no_tuples = (no_tuples as f64 / max_distincts) as i32;
            }
        }

        self.no_tuples = no_tuples;
    }

    pub fn index_of(&self, attribute: &str) -> Option<usize> {
        self.attributes
            .iter()
            .position(|attr| attr.name == attribute)
    }

    pub fn find_type(&self, attribute: &str) -> Option<Type> {
        self.index_of(attribute)
            .map(|index| self.attributes[index].type_)
    }

    pub fn get_distincts(&self, attribute: &str) -> Option<i32> {
        self.index_of(attribute)
            .map(|index| self.attributes[index].no_distinct)
    }

    pub fn set_distincts(&mut self, attribute: &str, no_distinct: i32) -> bool {
        self.index_of(attribute)
            .map(|index| {
                self.attributes[index].no_distinct = no_distinct;
            })
            .is_some()
    }

    pub fn rename_att(&mut self, old_name: &str, new_name: &str) -> bool {
        if self.index_of(new_name).is_some() {
            return false;
        }
        self.index_of(old_name)
            .map(|index| {
                self.attributes[index].name = new_name.to_string();
            })
            .is_some()
    }

    pub fn project(&mut self, atts_to_keep: &[i32]) -> bool {
        let new_attributes = atts_to_keep
            .iter()
            .map(|&i| self.attributes.get(i as usize))
            .collect::<Vec<_>>();

        if new_attributes.iter().any(|attr| attr.is_none()) {
            return false;
        }

        self.attributes = new_attributes
            .into_iter()
            .map(|attr| attr.unwrap().clone())
            .collect();

        true
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        for attr in &self.attributes {
            write!(f, "{}: {}", attr.name, attr.type_)?;
        }
        write!(f, ")")?;

        writeln!(f, "[{}][{}]", self.no_tuples, self.f_path)
    }
}
