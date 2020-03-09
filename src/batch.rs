use crate::{db, push_zip, Result};
use log::*;
use stopwatch::Stopwatch;

async fn download_url(url: hyper::Uri, filename: String) -> Result<String> {
    use async_std::io::prelude::*;
    use hyper::{body::HttpBody as _, Client, StatusCode};
    use std::io;

    let https = hyper_tls::HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);
    let sw = Stopwatch::start_new();

    info!("download start: filename={}", filename);

    let mut res = client.get(url).await?;

    if res.status() != StatusCode::OK {
        return Err(io::Error::new(io::ErrorKind::Other, "not 200").into());
    }

    let mut file = async_std::fs::File::create(&filename).await?;
    while let Some(next) = res.data().await {
        let chunk = next?;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    info!(
        "download finished: filename={}, elapsed={}ms",
        filename,
        sw.elapsed_ms()
    );

    Ok(filename)
}

pub fn import_urls(url_file: &str) -> Result<()> {
    use futures::prelude::*;
    use futures::stream::TryStreamExt;
    use futures::task::SpawnExt;

    let urls = std::fs::read_to_string(&url_file)?;

    let mut f_list = Vec::new();
    for url in urls.split("\n") {
        if url.is_empty() {
            continue;
        }

        let uri = url.parse::<hyper::Uri>().expect("uri.parse");
        let filename = std::path::Path::new(uri.path())
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let tmpdir = crate::tmpdir();
        let filepath = format!("{}/{}", tmpdir, filename);

        let blobs = db::by_filename(&filename)?;
        if blobs.is_empty() {
            f_list.push(download_url(uri, filepath))
        }
    }

    let pool = futures::executor::ThreadPool::new().expect("ThreadPool::new");
    let stream = stream::iter(f_list)
        .buffered(4)
        .and_then(|filename| {
            pool.spawn_with_handle(async move { push_zip(&filename) })
                .expect("spawn_with_handle")
        })
        .try_collect::<Vec<_>>();

    let mut runtime = tokio::runtime::Runtime::new().expect("Runtime::new");
    runtime.block_on(stream)?;

    Ok(())
}
