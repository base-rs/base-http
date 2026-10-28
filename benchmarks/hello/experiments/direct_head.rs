// Inserted only in the diagnostic ResponseWriter, before the general plan.
// This broad fixed-length 200 path retains input parsing, response construction,
// body/error/length checks and persistence. It avoids cloning application fields
// merely to append an implicit Content-Length to the serialized message.
if let Some(length) = response.body.length() {
    let version = exchange.head().version.unwrap_or(Version::Http11);
    let fields = &response.head.fields;
    if response.head.status == Status::OK
        && version == Version::Http11
        && response.head.version.is_none_or(|version| version == Version::Http11)
        && response.head.protocol.is_none()
        && exchange.head().method != Method::HEAD
        && exchange.head().method != Method::CONNECT
        && !fields.has_trailers()
        && !fields.headers().iter().any(|field| {
            ["content-length", "transfer-encoding", "trailer", "upgrade", "connection"]
                .iter().any(|name| field.name().eq_ignore_ascii_case(name))
        })
    {
        use std::io::Write;
        let persistent = exchange.is_persistent() && !exchange.is_abandoned();
        assert!(self.pending.is_empty());
        self.pending.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
        wire::append_fields(&mut self.pending, fields.headers());
        write!(&mut self.pending, "Content-Length: {length}\r\n")?;
        if !persistent {
            self.pending.extend_from_slice(b"Connection: close\r\n");
        }
        self.pending.extend_from_slice(b"\r\n");
        self.usable = false;
        self.write_body(
            &mut response.body, &mut response.head.fields,
            Framing::Fixed(length), false, cancellation,
        ).await?;
        self.usable = persistent;
        self.continue_sent = false;
        return Ok(persistent);
    }
}
