use argh::FromArgs;
use increstore::*;

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
    ListFiles(SubCommandListFiles),
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

#[derive(FromArgs, PartialEq, Debug)]
/// debug-list-files
#[argh(subcommand, name = "debug-list-files")]
struct SubCommandListFiles {
    #[argh(description = "roots", switch)]
    roots: bool,
    #[argh(description = "non-roots", switch)]
    non_roots: bool,
}

fn main() -> Result<()> {
    env_logger::init();

    std::fs::create_dir_all(prefix()).expect("failed to create dir");

    db::prepare().expect("failed to prepare");

    let up: TopLevel = argh::from_env();

    match up.nested {
        MySubCommandEnum::Push(cmd) => {
            push_zip(&cmd.filename)?;
        }
        MySubCommandEnum::Get(cmd) => {
            get(&cmd.filename, &cmd.out_filename)?;
        }
        MySubCommandEnum::BenchZip(cmd) => {
            bench_zip(&cmd.filename, cmd.parallel)?;
        }

        MySubCommandEnum::CleanUp(_cmd) => {
            cleanup()?;
        }
        MySubCommandEnum::Stats(_cmd) => {
            debug_stats()?;
        }
        MySubCommandEnum::Graph(cmd) => {
            debug_graph(&cmd.filename)?;
        }
        MySubCommandEnum::ListFiles(cmd) => {
            debug_list_files(cmd.roots, cmd.non_roots)?;
        }
    }

    Ok(())
}
