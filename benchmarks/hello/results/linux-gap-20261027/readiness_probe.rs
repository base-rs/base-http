use std::{io::Write, net::{TcpListener,TcpStream}, os::fd::AsRawFd, time::Duration};
use tokio::io::AsyncReadExt;
unsafe extern "C" {
 fn send(fd: i32, buffer: *const u8, len: usize, flags: i32) -> isize;
 fn recv(fd: i32, buffer: *mut u8, len: usize, flags: i32) -> isize;
}
fn main() {
 for consume in [false,true] {
  let listener=TcpListener::bind("127.0.0.1:0").unwrap();
  let mut peer=TcpStream::connect(listener.local_addr().unwrap()).unwrap();
  peer.set_nodelay(true).unwrap();
  let (native,_)=listener.accept().unwrap();native.set_nonblocking(true).unwrap();
  let alias=native.try_clone().unwrap();
  let rt=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
  rt.block_on(async {
   let mut stream=tokio::net::TcpStream::from_std(native).unwrap();let mut buffer=[0;64];
   peer.write_all(b"x").unwrap();assert_eq!(stream.read(&mut buffer).await.unwrap(),1);
   peer.write_all(b"before").unwrap();assert_eq!(unsafe {send(peer.as_raw_fd(), b"!".as_ptr(),1,1)},1);
   peer.write_all(b"after").unwrap();std::thread::sleep(Duration::from_millis(20));
   if consume {let mut byte=0;assert_eq!(unsafe{recv(alias.as_raw_fd(),&mut byte,1,1|0x40)},1);}
   let n=tokio::time::timeout(Duration::from_millis(200),stream.read(&mut buffer)).await.unwrap().unwrap();
   assert_eq!(&buffer[..n],b"before");
   let next=tokio::time::timeout(Duration::from_millis(200),stream.read(&mut buffer)).await;
   match next {
    Ok(Ok(n)) => println!("consume_urgent={consume}: next_read={:?}", &buffer[..n]),
    other => {let n=unsafe{recv(alias.as_raw_fd(),buffer.as_mut_ptr(),buffer.len(),0x40)};println!("consume_urgent={consume}: next_read={other:?}, independently_queued={n}, suffix={:?}", &buffer[..n.max(0) as usize]);}
   }
  });
 }
}
