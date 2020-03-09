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
    Exists(SubCommandExists),

    Dedytrate(SubCommandDehydrate),
    Hydrate(SubCommandHydrate),

    ImportUrls(SubCommandImportUrls),

    BenchZip(SubCommandBenchZip),

    CleanUp(SubCommandCleanUp),
    Stats(SubCommandStats),
    Graph(SubCommandGraph),
    ListFiles(SubCommandListFiles),
    Hash(SubCommandHash),
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

    #[argh(description = "dry-run", switch)]
    dry_run: bool,
}

#[derive(FromArgs, PartialEq, Debug)]
/// exists
#[argh(subcommand, name = "exists")]
struct SubCommandExists {
    #[argh(positional)]
    filename: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// dehydrate
#[argh(subcommand, name = "dehydrate")]
struct SubCommandDehydrate {}

#[derive(FromArgs, PartialEq, Debug)]
/// dehydrate
#[argh(subcommand, name = "hydrate")]
struct SubCommandHydrate {}

#[derive(FromArgs, PartialEq, Debug)]
/// import-urls
#[argh(subcommand, name = "import-urls")]
struct SubCommandImportUrls {
    #[argh(positional)]
    filename: String,
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
#[argh(subcommand, name = "debug-ls-files")]
struct SubCommandListFiles {
    #[argh(description = "roots", switch)]
    roots: bool,
    #[argh(description = "non-roots", switch)]
    non_roots: bool,
    #[argh(description = "long", switch, short = 'l')]
    long: bool,
}

#[derive(FromArgs, PartialEq, Debug)]
/// debug-list-files
#[argh(subcommand, name = "debug-hash")]
struct SubCommandHash {
    #[argh(positional)]
    filename: String,
}

fn main() -> Result<()> {
    env_logger::init();

    std::fs::create_dir_all(prefix()).expect("failed to create dir");

    db::prepare().expect("failed to prepare");

    let up: TopLevel = argh::from_env();

    match up.nested {
        MySubCommandEnum::Push(cmd) => push_zip(&cmd.filename),
        MySubCommandEnum::Get(cmd) => get(&cmd.filename, &cmd.out_filename, cmd.dry_run),
        MySubCommandEnum::Exists(cmd) => exists(&cmd.filename),

        MySubCommandEnum::Dedytrate(_cmd) => dehydrate(),
        MySubCommandEnum::Hydrate(_cmd) => hydrate(),

        MySubCommandEnum::ImportUrls(cmd) => import_urls(&cmd.filename),

        MySubCommandEnum::BenchZip(cmd) => bench_zip(&cmd.filename, cmd.parallel),

        MySubCommandEnum::CleanUp(_cmd) => cleanup(),
        MySubCommandEnum::Stats(_cmd) => debug_stats(),
        MySubCommandEnum::Graph(cmd) => debug_graph(&cmd.filename),
        MySubCommandEnum::ListFiles(cmd) => debug_list_files(cmd.roots, cmd.non_roots, cmd.long),
        MySubCommandEnum::Hash(cmd) => debug_hash(&cmd.filename),
    }
}
