- `#define SWAP(a, b) { typeof(a) tmp = a; a = b; b = tmp; }`
  - Also important to note: `typeof` is a GCC extension and is not standard C++
    - `decltype` and `auto` are standard in C++ and world also work here
  - You should just use `std::swap` from `<utility>`
  - I'll use Rust's `std::mem::swap` in the Rust version
- `Swapify` and the custom Vector/LinkedList classes are unnecessary
  - The methods on `Swapify` can be done trivially in singular expressions already
    - ie. instead of `a.swap(b)` you can just do `std::swap(a, b)`
  - There's already `std::vector` and `std::list`
- for `Schema::Schema(StringVector& _attributes, StringVector& _attributeTypes, IntVector& _distincts)` Attribute.type will be left uninitialized if the corresponding attribute type doesn't match any of the expected strings
  - In the Rust port, I am panicking if something invalid is passed, but I think it's important to note that the C++ version does not do this, so if there's some usage of an uninitialized Attribute.type (which I assume is a mistake), it could break the rust port
- `Schema::FindType` returns `Type::Integer` when there is no attribute with the given name.
  - I'm assuming this is done in because of a lack of result types built into C++, but in Rust I will be returning an `Option<Type>` instead
  - This might cause errors in the Rust port if it is relied on in some way, I doubt it is, though
- The implementation of `<<` uses `cout` directly instead of the provided `ostream& _os` for INTEGER, STRING, and UNKNOWN. The Rust implementation writes to the formatter directly, as I assume this is what the intended behavior was.
- `Record::ExtractNextRecord` deletes the current before reading the new data, which results in Record.bits being `NULL` after a failure to read the file. In the Rust implementation, it currently does not do that. Instead, it only nulls out the `bits` on a success as I assume that this was not intended behavior.
  - Also, allocating a full 262 kb every time no matter the size of the record is pretty wasteful.
- `Type::Name` seems to be relatively consistently ignored by everything, not sure what's going on there
- The implementation of `Comparison::Run` does not handle "Target::Literal" at all, not sure what's up with that
- `OrderMaker::OrderMaker(Schema& schema)` iterates over all attributes of the schema 3 times for no apparent reason

```
//first add the Integer attributes
for (int i = 0; i < n; i++) {
  if (atts[i].type == Integer) {
    whichAtts[numAtts] = i;
    whichTypes[numAtts] = Integer;
    numAtts++;
  }
}

// now add in the doubles
for (int i = 0; i < n; i++) {
  if (atts[i].type == Float) {
    whichAtts[numAtts] = i;
    whichTypes[numAtts] = Float;
    numAtts++;
  }
}

// and finally the strings
for (int i = 0; i < n; i++) {
  if (atts[i].type == String) {
    whichAtts[numAtts] = i;
    whichTypes[numAtts] = String;
    numAtts++;
  }
}
```

Instead of just doing it all in one pass

```
for (int i = 0; i < n; i++) {
  whichAtts[numAtts] = i;
  whichTypes[numAtts] = atts[i].type;
  numAtts++;
}
```
