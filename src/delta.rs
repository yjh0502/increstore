use crate::rw::*;
use std::marker::Unpin;
use std::path::Path;

pub use xdelta3::stream::ProcessMode;

#[allow(unused)]
pub fn delta<R1, R2, W>(
    op: xdelta3::stream::ProcessMode,
    src_reader: R1,
    input_reader: R2,
    dst: W,
) -> std::io::Result<WriteMetadata>
where
    R1: std::io::Read + Unpin,
    R2: std::io::Read + Unpin,
    W: std::io::Write + Unpin,
{
    let mut dst = HashRW::new(dst);

    let cfg = xdelta3::stream::Xd3Config::new()
        .source_window_size(100_000_000)
        .no_compress(true)
        .level(0);

    xdelta3::stream::process(cfg, op, input_reader, src_reader, &mut dst)?;

    let dst_meta = dst.meta();

    Ok(dst_meta)
}

/// uses std::io::Result to trigger TimedOut
pub fn delta_file<P1, P2, P3>(
    op: xdelta3::stream::ProcessMode,
    src_filename: P1,
    input_filename: P2,
    dst_filename: P3,
) -> std::io::Result<Option<WriteMetadata>>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
    P3: AsRef<Path>,
{
    let handle = delta_file_handle(op, src_filename, input_filename, dst_filename)?;
    handle.meta()
}

pub struct DeltaSpawn {
    child: Option<std::process::Child>,
    output: Option<std::process::Output>,

    meta: Option<WriteMetadata>,
    dst_filepath: std::path::PathBuf,
}

impl DeltaSpawn {
    fn new<P: AsRef<Path>>(child: std::process::Child, dst_filename: P) -> Self {
        DeltaSpawn {
            child: Some(child),
            output: None,

            meta: None,
            dst_filepath: dst_filename.as_ref().to_path_buf(),
        }
    }

    #[allow(unused)]
    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        match (&mut self.child, &self.output) {
            (_, Some(ref output)) => {
                // already completed
                Ok(Some(output.status.clone()))
            }
            (Some(ref mut child), None) => {
                // child process is still running
                Ok(child.try_wait()?)
            }
            _ => {
                panic!("DeltaSpawn::try_wait called on an invalid state");
            }
        }
    }

    pub fn meta(mut self) -> std::io::Result<Option<WriteMetadata>> {
        if let Some(ref meta) = self.meta {
            return Ok(Some(meta.clone()));
        }

        let child = match self.child.take() {
            Some(child) => child,
            None => {
                return Ok(None);
            }
        };

        let output = child.wait_with_output()?;
        if !output.status.success() {
            eprintln!(
                "xdelta3: {}/{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "xdelta3 process did not exit successfully",
            ));
        }

        // calculate metadata
        let dstfile = std::fs::File::open(&self.dst_filepath)?;
        let mut dstfile = std::io::BufReader::new(dstfile);
        let mut hasher = HashRW::new(std::io::sink());
        std::io::copy(&mut dstfile, &mut hasher)?;

        self.output = Some(output);
        self.meta = Some(hasher.meta());
        Ok(Some(self.meta.clone().unwrap()))
    }

    pub fn kill(&mut self) -> std::io::Result<()> {
        if let Some(ref mut child) = self.child.take() {
            child.kill()?;
        }
        Ok(())
    }
}

impl Drop for DeltaSpawn {
    fn drop(&mut self) {
        // ensure the child process is killed if not already
        let _ = self.kill();
    }
}

/// uses std::io::Result to trigger TimedOut
pub fn delta_file_handle<P1, P2, P3>(
    op: xdelta3::stream::ProcessMode,
    src_filename: P1,
    input_filename: P2,
    dst_filename: P3,
) -> std::io::Result<DeltaSpawn>
where
    P1: AsRef<std::path::Path>,
    P2: AsRef<std::path::Path>,
    P3: AsRef<std::path::Path>,
{
    use std::process::Command;
    // spawn command
    // xdelta3 -ds src_file input_file dst_file

    std::fs::remove_file(dst_filename.as_ref()).ok();

    let output = match op {
        ProcessMode::Decode => Command::new("hpatchz")
            .arg(src_filename.as_ref())
            .arg(input_filename.as_ref())
            .arg(dst_filename.as_ref())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn(),
        ProcessMode::Encode => Command::new("hdiffz")
            .arg("-s")
            .arg("-SD")
            .arg("-c-zstd-21-24")
            .arg(src_filename.as_ref())
            .arg(input_filename.as_ref())
            .arg(dst_filename.as_ref())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn(),
    }?;

    /*
        let arg = match op {
            ProcessMode::Decode => "-ds",
            ProcessMode::Encode => "-es",
        };

        let output = Command::new("xdelta3")
            .arg(arg)
            .arg(src_filename.as_ref())
            .arg(input_filename.as_ref())
            .arg(dst_filename.as_ref())
            .spawn()?;
    */
    Ok(DeltaSpawn::new(output, dst_filename))
}
