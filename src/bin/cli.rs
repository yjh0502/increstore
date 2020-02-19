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
    One(SubCommandPush),
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
    increstore::db::prepare().expect("failed to prepare");

    let up: TopLevel = argh::from_env();

    match up.nested {
        MySubCommandEnum::One(push) => {
            increstore::push_zip(&push.filename)?;
        }
    }

    Ok(())
}
