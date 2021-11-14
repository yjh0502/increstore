# increstore

## build

```sh
sudo apt install libsqlite3-dev
cargo build --release
```

## add new files
```sh
# use default WORKDIR=data
cargo run --release -- push FILENAME
# or, specify WORKDIR
WORKDIR=/data/workdir cargo run --release -- push FILENAME
# or, execute binary intead of using cargo-run
./target/release/cli push FILENAME
```

## help

```sh
cargo run --release -- --help
```
