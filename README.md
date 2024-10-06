
# Log structured merge tree in Rust

Log structured merge tree implementation in rust from [mini-lsm](https://skyzh.github.io/mini-lsm/00-preface.html)

Usage (cargo and rust required):

build
```
cargo build
```

run cli
```
cargo run --bin mini-lsm-cli -- --compaction none
```

commands:

- put key:String value: String  | Put key value in db
- get key:String                | Get key from db
- fill begin end                | Fill begin-end with key=value
- scan begin end | Scan keys from begin-end in sorted order
- dump | dump the tree structure and show tables
- flush | flush a memtable to disk
- close | exit cli