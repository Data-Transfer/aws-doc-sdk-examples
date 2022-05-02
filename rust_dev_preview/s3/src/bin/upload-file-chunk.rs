use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint, Error};
use std::path::Path;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};
/// # Upload file chunk
///
/// ## Shows how to:
///
/// * read a chunk of data from a file using:
///    - `tokio::fs::File::seek`
///    - `tokio::io::AsyncReadExt::take (added to tokio::fs::File,
///                                    limits the number bytes read)`
/// * minimize memory usage through tokio_util::FramedRead/BytesCodec which
/// .  reuse an internal bytes::BytesMut to store data
/// * upload the chunk to an S3 endpoint
/// * extract and print returned etag
///
/// usage:
/// ```shell
/// ./upload-file-chunk <profile> <url> <bucket> <key> <input file> \
/// <start offset> <chunk size, 0 for whole file>
/// ```
#[tokio::main]
async fn main() -> Result<(), aws_sdk_s3::Error> {
    let args = std::env::args().collect::<Vec<_>>();
    let usage = format!("{} <profile> <url> <bucket> <key> <input file> <start offset> <chunk size, 0 for whole file>", args[0]);
    let profile = args.get(1).expect(&usage);
    let url = args.get(2).expect(&usage);
    let bucket = args.get(3).expect(&usage);
    let key = args.get(4).expect(&usage);
    let file_name = args.get(5).expect(&usage);
    let start_offset = args
        .get(6)
        .expect(&usage)
        .parse::<u64>()
        .expect("Error parsing offset");
    let chunk_size = args
        .get(7)
        .expect(&usage)
        .parse::<u64>()
        .expect("Error parsing chunk size");
    let chunk_size = if chunk_size == 0 {
        let md = std::fs::metadata(file_name).map_err(|err| Error::Unhandled(Box::new(err)))?;
        md.len()
    } else {
        chunk_size
    };

    // credentials are read from .aws/credentials file
    let conf = aws_config::from_env()
        .region("us-east-1")
        .credentials_provider(
            aws_config::profile::ProfileFileCredentialsProvider::builder()
                .profile_name(profile)
                .build(),
        )
        .load()
        .await;
    let uri = url.parse::<http::uri::Uri>().expect("Invalid URL");
    let ep = Endpoint::immutable(uri);
    let s3_conf = aws_sdk_s3::config::Builder::from(&conf)
        .endpoint_resolver(ep)
        .build();
    let client = Client::from_conf(s3_conf);
    upload_chunk(&client, &bucket, &key, &file_name, start_offset, chunk_size).await?;
    Ok(())
}

/// Upload file chunk to bucket/key; uses framed read to minimize copies
pub async fn upload_chunk(
    client: &Client,
    bucket: &str,
    key: &str,
    file_name: &str,
    start_offset: u64,
    chunk_size: u64,
) -> Result<(), Error> {
    // minimize memory copies https://github.com/hyperium/hyper/issues/2166#issuecomment-612363623
    let mut file = tokio::fs::File::open(Path::new(file_name))
        .await
        .map_err(|err| Error::Unhandled(Box::new(err)))?;
    file.seek(std::io::SeekFrom::Start(start_offset))
        .await
        .map_err(|err| Error::Unhandled(Box::new(err)))?;
    let file = file.take(chunk_size);
    let stream = FramedRead::with_capacity(file, BytesCodec::new(), chunk_size as usize);
    let b = hyper::Body::wrap_stream(stream);
    let body = ByteStream::from(b);
    let start = Instant::now();
    let resp = client
        .put_object()
        .content_length(chunk_size as i64)
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    let elapsed = start.elapsed();
    match resp.e_tag() {
        Some(etag) => println!("etag: {}", etag.trim_matches('"')),
        None => eprintln!("No etag in response"),
    }
    println!(
        "Uploaded chunk of size {} from file {} in {:.2} s",
        chunk_size,
        file_name,
        elapsed.as_secs_f32()
    );
    Ok(())
}
