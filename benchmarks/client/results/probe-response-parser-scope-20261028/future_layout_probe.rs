use base_endpoint::stream::ReadableStream;
use base_http::{Method, RequestMetadata, InterimResponses, ResponseMetadata};
use base_http1::ResponseReader;
use base_scheduler::{Cancellation, OperationResult};
use std::{io, convert::Infallible, future::pending};
struct Input;
impl ReadableStream for Input {
    async fn read_into(&mut self, _: &mut [u8], _: &Cancellation) -> OperationResult<usize> { pending().await }
    async fn close(&mut self) -> io::Result<()> { Ok(()) }
}
#[test]
fn print_response_future_layout() {
    let mut reader = ResponseReader::new(Input, Default::default()).unwrap();
    let request = RequestMetadata::new(Method::GET);
    let mut observer = InterimResponses::new(|_: ResponseMetadata| async { Ok::<_, Infallible>(()) });
    let cancel = Cancellation::never();
    let future = reader.read_response(&request, &mut observer, &cancel);
    println!("response read future: {} bytes", std::mem::size_of_val(&future));
}
