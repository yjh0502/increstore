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
    Get(SubCommandGet),
    BenchZip(SubCommandBenchZip),

    CleanUp(SubCommandCleanUp),
    Stats(SubCommandStats),
    Graph(SubCommandGraph),
}

#[derive(FromArgs, PartialEq, Debug)]
/// push
#[argh(subcommand, name = "push")]
struct SubCommandPush {
    #[argh(positional)]
    filename: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// get
#[argh(subcommand, name = "get")]
struct SubCommandGet {
    #[argh(positional)]
    filename: String,

    #[argh(positional)]
    out_filename: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// bench-zip
#[argh(subcommand, name = "bench-zip")]
struct SubCommandBenchZip {
    #[argh(description = "parallel", switch, short = 'p')]
    parallel: bool,
    #[argh(positional)]
    filename: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// cleanup
#[argh(subcommand, name = "debug-cleanup")]
struct SubCommandCleanUp {}

#[derive(FromArgs, PartialEq, Debug)]
/// debug-depth
#[argh(subcommand, name = "debug-stats")]
struct SubCommandStats {}

#[derive(FromArgs, PartialEq, Debug)]
/// debug-graph
#[argh(subcommand, name = "debug-graph")]
struct SubCommandGraph {
    #[argh(positional)]
    filename: String,
}

fn main() -> std::io::Result<()> {
    env_logger::init();

    std::fs::create_dir_all(increstore::prefix()).expect("failed to create dir");

    increstore::db::prepare().expect("failed to prepare");

    let up: TopLevel = argh::from_env();

    match up.nested {
        MySubCommandEnum::Push(cmd) => {
            increstore::push_zip(&cmd.filename)?;
        }
        MySubCommandEnum::Get(cmd) => {
            increstore::get(&cmd.filename, &cmd.out_filename)?;
        }
        MySubCommandEnum::BenchZip(cmd) => {
            increstore::bench_zip(&cmd.filename, cmd.parallel)?;
        }

        MySubCommandEnum::CleanUp(_cmd) => {
            increstore::cleanup()?;
        }
        MySubCommandEnum::Stats(_cmd) => {
            increstore::debug_stats()?;
        }
        MySubCommandEnum::Graph(cmd) => {
            increstore::debug_graph(&cmd.filename)?;
        }
    }

    Ok(())
}
