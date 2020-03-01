use argh::FromArgs;

#[derive(FromArgs, PartialEq, Debug)]
/// Top-level command.
struct TopLevel {
    #[argh(subcommand)]
    nested: MySubCommandEnum,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum MySubCommandEnum {
    Push(SubCommandPush),
}

#[derive(FromArgs, PartialEq, Debug)]
/// First subcommand.
#[argh(subcommand, name = "push")]
struct SubCommandPush {
    #[argh(positional)]
    filename: String,
}

fn main() -> std::io::Result<()> {
    env_logger::init();

    std::fs::create_dir_all(increstore::prefix()).expect("failed to create dir");

    increstore::db::prepare().expect("failed to prepare");

    let up: TopLevel = argh::from_env();

    match up.nested {
        MySubCommandEnum::Push(push) => {
            increstore::push_zip(&push.filename)?;
        }
    }

    Ok(())
}
