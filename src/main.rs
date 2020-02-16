use std::io;

fn zip_to_tar(src_filename: &str, dst_filename: &str) -> io::Result<()> {
    let src_file = std::fs::File::open(src_filename)?;
    let dst_file = std::fs::File::create(dst_filename)?;

    let mut zip = zip::ZipArchive::new(src_file)?;
    let mut ar = tar::Builder::new(dst_file);

    for i in 0..zip.len() {
        let mut file = zip.by_index(i)?;
        let filename = file.name().to_owned();

        let mut header = tar::Header::new_gnu();
        header.set_path(&filename)?;
        header.set_size(file.size());

        if let Some(mode) = file.unix_mode() {
            header.set_mode(mode);
        } else {
            if file.is_dir() {
                header.set_mode(0o755);
            } else {
                header.set_mode(0o644);
            }
        }

        let unixtime = file.last_modified().to_time().to_timespec().sec;
        header.set_mtime(unixtime as u64);

        header.set_cksum();

        ar.append(&mut header, &mut file)?;
    }

    Ok(())
}

fn main() {
    zip_to_tar("data/test.apk", "data/test.tar").expect("failed to convert");
    println!("Hello, world!");
}
