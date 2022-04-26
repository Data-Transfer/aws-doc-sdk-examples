use aws_sdk_s3::model::CompletedMultipartUpload;
use aws_sdk_s3::model::CompletedPart;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint, Error};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};
/// Multipart upload example
///
/// ## Usage
/// ```shell
/// upload-file-multipart <profile> <url> <bucket> <key> <input file> \
///   <number of parts> [optional read buffer size]
/// ```
///
#[tokio::main]
async fn main() -> Result<(), aws_sdk_s3::Error> {
    const REGION: &str = "us-east-1";
    let args = std::env::args().collect::<Vec<_>>();
    let usage = format!(
        "{} <profile> <url> <bucket> <key> <input file> <number of parts> [buffer size]",
        args[0]
    );
    let profile = args.get(1).expect(&usage);
    let url = args.get(2).expect(&usage);
    let bucket = args.get(3).expect(&usage);
    let key = args.get(4).expect(&usage);
    let file_name = args.get(5).expect(&usage);
    let num_parts = args
        .get(6)
        .expect(&usage)
        .parse::<usize>()
        .expect("Error parsing num parts");
    let buffer_capacity = if let Some(arg) = args.get(7) {
        Some(arg.parse::<usize>().expect("Wrong buffer size format"))
    } else {
        None
    };
    // credentials are read from .aws/credentials file
    let conf = aws_config::from_env()
        .region(REGION)
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
    upload_multipart(
        &client,
        &bucket,
        &key,
        &file_name,
        num_parts,
        buffer_capacity,
    )
    .await?;
    Ok(())
}

/// Multipart upload
/// 
/// 1. retrieve `upload id`
/// 2. iterate over file chunks and send each chunk as a separate part
/// 3. store returned `etag` and `part number` into `Vec`
/// 4. complete upload by sending list of `(etag, part id`) to server
/// 5. print returned `etag` id
pub async fn upload_multipart(
    client: &Client,
    bucket: &str,
    key: &str,
    file_name: &str,
    num_parts: usize,
    buffer_capacity: Option<usize>, // None for default
) -> Result<(), Error> {
    let len: u64 = std::fs::metadata(file_name)
        .map_err(|err| Error::Unhandled(Box::new(err)))?
        .len();
    let num_parts = num_parts as u64;
    let file = tokio::fs::File::open(file_name)
        .await
        .map_err(|err| Error::Unhandled(Box::new(err)))?;
    let chunk_size = len / num_parts;
    let last_chunk_size = chunk_size + len % num_parts;
    // Initiate multipart upload and store upload id.
    let u = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    let uid = u.upload_id().ok_or(Error::NoSuchUpload(
        aws_sdk_s3::error::NoSuchUpload::builder()
            .message("No upload ID")
            .build(),
    ))?;
    // Iterate over file chunks, changing the file pointer at each iteration
    // and storing returned part id and associated etag into vector.
    let mut completed_parts: Vec<CompletedPart> = Vec::new();
    for i in 0..num_parts {
        let size = if i != (num_parts - 1) {
            chunk_size
        } else {
            last_chunk_size
        };
        let mut file = file
            .try_clone()
            .await
            .map_err(|err| Error::Unhandled(Box::new(err)))?;
        file.seek(std::io::SeekFrom::Start((i * len / num_parts) as u64))
            .await
            .map_err(|err| Error::Unhandled(Box::new(err)))?;
        let file_chunk = file.take(size);
        let stream = if let Some(capacity) = buffer_capacity {
            FramedRead::with_capacity(file_chunk, BytesCodec::new(), capacity)
        } else {
            FramedRead::new(file_chunk, BytesCodec::new())
        };
        let b = hyper::Body::wrap_stream(stream);
        let body = ByteStream::from(b);
        let up = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .content_length(size as i64)
            .upload_id(uid.clone())
            .part_number((i + 1) as i32)
            .body(body)
            .send()
            .await?;
        let cp = CompletedPart::builder()
            .set_e_tag(up.e_tag)
            .part_number((i + 1) as i32)
            .build();
        completed_parts.push(cp);
    }
    // Complete multipart upload, sending the (etag, part id) list along the request.
    let b = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();
    let completed = client
        .complete_multipart_upload()
        .multipart_upload(b)
        .upload_id(uid.clone())
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    // Print etag removing quotes.
    if let Some(etag) = completed.e_tag {
        println!("{}", etag.replace("\"", ""));
    } else {
        eprintln!("No etag received");
    }
    Ok(())
}
