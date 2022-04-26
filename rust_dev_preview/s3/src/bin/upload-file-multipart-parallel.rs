use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint, Error};
use aws_sdk_s3::model::CompletedMultipartUpload;
use aws_sdk_s3::model::CompletedPart;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};
/// Parallel multipart upload, one task per part.
///
/// ## Usage
/// ```
/// upload-file-multipart-parallel <profile> <url> <bucket> <key> \
///   <input file> <number of parts> [optional read buffer size]
/// ```
///
#[tokio::main]
async fn main() -> Result<(), aws_sdk_s3::Error> {
    const REGION: &str = "us-east-1";
    let args = std::env::args().collect::<Vec<_>>();
    let usage = format!("{} <profile> <url> <bucket> <key> <input file> <number of parts>", args[0]);
    let profile = args.get(1).expect(&usage);
    let url = args.get(2).expect(&usage);
    let bucket = args.get(3).expect(&usage);
    let key = args.get(4).expect(&usage);
    let file_name = args.get(5).expect(&usage);
    let num_parts = args.get(6).expect(&usage).parse::<usize>().expect("Error parsing num parts");
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
    let start = Instant::now();
    upload_multipart_parallel(&client, &bucket, &file_name, &key, num_parts, buffer_capacity).await?;
    let elapsed = start.elapsed();
    println!("Uploaded file in {:.2} s", elapsed.as_secs_f32());
    Ok(())
}
//  to set number of threads:
//    let mut rt = runtime::Builder::new()
//                 .core_threads(4)
//                 .build()
//                 .unwrap();
//    rt.spawn(...);             
/// Parallel multipart upload, one task per part.
pub async fn upload_multipart_parallel(
    client: &Client,
    bucket: &str,
    file_name: &str,
    key: &str,
    num_parts: usize,
    buffer_capacity: Option<usize>
) -> Result<(), Error> {
    let len: u64 = std::fs::metadata(file_name).map_err(|err| Error::Unhandled(Box::new(err)))?.len();
    let num_parts = num_parts as u64;
    let chunk_size = len / num_parts;
    let last_chunk_size = chunk_size + len % num_parts;
    
    // Initiate multipart upload and store upload id.
    let u = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    let uid = u.upload_id().ok_or(
        Error::NoSuchUpload(aws_sdk_s3::error::NoSuchUpload::builder().message("No upload ID").build()))?;
    // Iterate over file chunks, changing the file pointer at each iteration
    // and storing part id and associated etag into vector.
    let mut handles = Vec::new();
    for i in 0..num_parts {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let part_id = (i + 1) as i32;
        let size = if i != (num_parts - 1) { chunk_size } else { last_chunk_size };
        let offset = (i * len / num_parts) as u64;
        let uid = uid.to_string();
        let file_name = file_name.to_string();
    
        let cp = tokio::spawn(async move {
            upload_part(client, file_name, bucket, key, part_id, uid, offset, size, buffer_capacity)
        });
        handles.push(cp);
    }
    let mut completed_parts = Vec::new();
    for h in handles {
        let p = h.await.map_err(|err| Error::Unhandled(Box::new(err)))?.await?;
        completed_parts.push(p);
    }
    // Complete multipart upload, sending the (etag, part id) list along the request.
    let b = CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build();
    let completed = client.complete_multipart_upload().multipart_upload(b).
                    upload_id(uid.clone()).bucket(bucket).key(key).send().await?;
    // Print etag removing quotes.
    if let Some(etag) = completed.e_tag {
        println!("{}", etag.replace("\"",""));
    } else {
        eprintln!("Error receiving etag");
    }
    Ok(())
    
}

/// Upload single plart and return etag
async fn upload_part(client: Client, file_name: String, bucket: String, 
                     key: String, part_num: i32, uid: String,
                     offset: u64, size: u64, buffer_capacity: Option<usize>)
                     -> Result<CompletedPart, Error> {

    let mut file = tokio::fs::File::open(file_name)
        .await
        .map_err(|err| Error::Unhandled(Box::new(err)))?;
    file.seek(std::io::SeekFrom::Start(offset))
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
        .upload_id(uid)
        .part_number(part_num)
        .body(body)
        .send()
        .await?;
    let cp = CompletedPart::builder().set_e_tag(up.e_tag).part_number(part_num).build();
    Ok(cp)
}

