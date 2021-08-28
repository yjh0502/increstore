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
    Exists(SubCommandExists),

    Rename(SubCommandRename),

    Dedytrate(SubCommandDehydrate),
    Hydrate(SubCommandHydrate),

    Archive(SubCommandArchive),

    Validate(SubCommandValidate),

    BenchZip(SubCommandBenchZip),

    CleanUp(SubCommandCleanUp),
    Stats(SubCommandStats),
    Graph(SubCommandGraph),
    ListFiles(SubCommandListFiles),
    Blobs(SubCommandBlobs),
    Hash(SubCommandHash),
}

/// push a version to archive
#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "push")]
struct SubCommandPush {
    #[argh(positional)]
    filename: String,

    #[argh(description = "zip", switch)]
    is_zip: bool,
    #[argh(description = "gz", switch)]
    is_gz: bool,
}

#[derive(FromArgs, PartialEq, Debug)]
/// get a version from archive
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
/// rename a existing version to new name
#[argh(subcommand, name = "rename")]
struct SubCommandRename {
    #[argh(positional)]
    from_filename: String,
    #[argh(positional)]
    to_filename: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// check if a version with given name already exists in archive
#[argh(subcommand, name = "exists")]
struct SubCommandExists {
    #[argh(positional)]
    filename: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// Remove all frontier versions from archive. The archive should be hydrated before adding a new
/// version. You can still able to get a existing version from archive.
#[argh(subcommand, name = "dehydrate")]
struct SubCommandDehydrate {}

#[derive(FromArgs, PartialEq, Debug)]
/// Restore all frontier version from archive. It will allow dehydrated archive to add new version.
#[argh(subcommand, name = "hydrate")]
struct SubCommandHydrate {}

#[derive(FromArgs, PartialEq, Debug)]
/// Create a tar archive from archive. The tar archive contains dehydrated archive.
#[argh(subcommand, name = "archive")]
struct SubCommandArchive {
    #[argh(positional)]
    filename: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// Get all versions from archive and validate checksum.
#[argh(subcommand, name = "validate")]
struct SubCommandValidate {}

#[derive(FromArgs, PartialEq, Debug)]
/// bench-zip. for dev.
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
/// Print statistics of archive.
#[argh(subcommand, name = "debug-stats")]
struct SubCommandStats {}

#[derive(FromArgs, PartialEq, Debug)]
/// Write graphviz graph of archive.
#[argh(subcommand, name = "debug-graph")]
struct SubCommandGraph {
    #[argh(positional)]
    filename: String,
}

#[derive(FromArgs, PartialEq, Debug)]
/// debug-list-files
#[argh(subcommand, name = "debug-ls-files")]
struct SubCommandListFiles {
    #[argh(description = "genesis", switch)]
    genesis: bool,
    #[argh(description = "roots", switch)]
    roots: bool,
    #[argh(description = "non-roots", switch)]
    non_roots: bool,
    #[argh(description = "long", switch, short = 'l')]
    long: bool,
}

#[derive(FromArgs, PartialEq, Debug)]
/// debug-blobs
#[argh(subcommand, name = "debug-blobs")]
struct SubCommandBlobs {}

#[derive(FromArgs, PartialEq, Debug)]
/// debug-hash
#[argh(subcommand, name = "debug-hash")]
struct SubCommandHash {
    #[argh(positional)]
    filename: String,
}

fn main() -> increstore::Result<()> {
    use increstore::*;

    env_logger::init();

    std::fs::create_dir_all(prefix()).expect("failed to create dir");

    let mut conn = db::open()?;
    let conn = &mut conn;
    db::prepare(conn).expect("failed to prepare");

    let up: TopLevel = argh::from_env();

    match up.nested {
        MySubCommandEnum::Push(cmd) => {
            let ty = match (cmd.is_zip, cmd.is_gz) {
                (true, true) => {
                    panic!("should not specify both zip and gz");
                }
                (true, false) => FileType::Zip,
                (false, true) => FileType::Gz,
                (false, false) => {
                    let path = std::path::Path::new(&cmd.filename);
                    if let Some(ext) = path.extension() {
                        if ext == "zip" || ext == "apk" || ext == "aab" {
                            FileType::Zip
                        } else if ext == "gz" {
                            FileType::Gz
                        } else if ext == "tar" {
                            FileType::Plain
                        } else {
                            panic!("unknown extension: {:?}", ext);
                        }
                    } else {
                        panic!("unknown extension: {}", cmd.filename);
                    }
                }
            };
            push(conn, &cmd.filename, ty)
        }
        MySubCommandEnum::Get(cmd) => get(conn, &cmd.filename, &cmd.out_filename, cmd.dry_run),
        MySubCommandEnum::Exists(cmd) => exists(conn, &cmd.filename),

        MySubCommandEnum::Rename(cmd) => rename(conn, &cmd.from_filename, &cmd.to_filename),

        MySubCommandEnum::Dedytrate(_cmd) => dehydrate(conn),
        MySubCommandEnum::Hydrate(_cmd) => hydrate(conn),

        MySubCommandEnum::Archive(cmd) => archive(conn, &cmd.filename),

        MySubCommandEnum::Validate(_cmd) => validate(conn),

        MySubCommandEnum::BenchZip(cmd) => bench_zip(&cmd.filename, cmd.parallel),

        MySubCommandEnum::CleanUp(_cmd) => cleanup(conn),
        MySubCommandEnum::Stats(_cmd) => debug_stats(conn),
        MySubCommandEnum::Graph(cmd) => debug_graph(conn, &cmd.filename),
        MySubCommandEnum::ListFiles(cmd) => {
            debug_list_files(conn, cmd.genesis, cmd.roots, cmd.non_roots, cmd.long)
        }
        MySubCommandEnum::Blobs(_cmd) => debug_blobs(conn),
        MySubCommandEnum::Hash(cmd) => debug_hash(&cmd.filename),
    }
}
