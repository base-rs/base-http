use std::{convert::Infallible, os::fd::AsRawFd, time::{Duration,Instant}};
use tokio::io::{AsyncReadExt,AsyncWriteExt};
use hyper::{server::conn::http1,service::service_fn,body::Bytes};
use hyper_util::rt::{TokioIo,TokioTimer};
use http_body_util::Full;
unsafe extern "C" {fn send(fd:i32,buf:*const u8,len:usize,flags:i32)->isize;}
async fn response(stream:&mut tokio::net::TcpStream)->std::io::Result<Vec<u8>> {
 let mut result=Vec::new();let mut bytes=[0;1024];
 loop {let n=stream.read(&mut bytes).await?;if n==0{return Ok(result)};result.extend_from_slice(&bytes[..n]);if result.ends_with(b"Hello, world!"){return Ok(result)}}
}
fn main(){
 for (oob,deadline) in [(false,false),(true,false),(true,true)] {
 let rt=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
 rt.block_on(async {
 let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
 let mut peer=tokio::net::TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();peer.set_nodelay(true).unwrap();
 let (socket,_)=listener.accept().await.unwrap();socket.set_nodelay(true).unwrap();
 let server=tokio::spawn(async move {
  let mut builder=http1::Builder::new();builder.auto_date_header(false);
  if deadline {builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_millis(100));}
  else {builder.header_read_timeout(None);}
  builder.serve_connection(TokioIo::new(socket),service_fn(|_|async{Ok::<_,Infallible>(hyper::Response::new(Full::new(Bytes::from_static(b"Hello, world!"))))})).await
 });
 peer.write_all(b"GET /prime HTTP/1.1\r\nHost: localhost\r\n\r\n").await.unwrap();
 assert!(response(&mut peer).await.unwrap().starts_with(b"HTTP/1.1 200"));
 tokio::task::yield_now().await;tokio::task::yield_now().await;
 let prefix=b"GET /target HTTP/1.1\r\nHost: localhost\r\n";
 assert_eq!(unsafe{send(peer.as_raw_fd(),prefix.as_ptr(),prefix.len(),0x40|0x4000)},prefix.len() as isize);
 if oob {assert_eq!(unsafe{send(peer.as_raw_fd(),b"!".as_ptr(),1,1|0x40|0x4000)},1);}
 assert_eq!(unsafe{send(peer.as_raw_fd(),b"\r\n".as_ptr(),2,0x40|0x4000)},2);
 std::thread::sleep(Duration::from_millis(20));
 let start=Instant::now();
 let result=tokio::time::timeout(Duration::from_millis(400),response(&mut peer)).await;
 match result {Ok(Ok(bytes))=>println!("oob={oob} header_deadline={deadline}: elapsed={:?}, reply={:?}", start.elapsed(),String::from_utf8_lossy(&bytes)),other=>println!("oob={oob} header_deadline={deadline}: elapsed={:?}, result={other:?}",start.elapsed())}
 if server.is_finished(){println!("server result: {:?}",server.await);}else{server.abort();let _=server.await;}
 });
 }
}
