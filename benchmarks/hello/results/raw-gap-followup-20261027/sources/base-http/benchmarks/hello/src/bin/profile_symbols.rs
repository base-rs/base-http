// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::io::{self, BufRead, Write};

fn main() -> io::Result<()> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    for line in io::stdin().lock().lines() {
        let line = line?;
        if let Some(start) = line.find("_R") {
            let end = line[start..]
                .find(char::is_whitespace)
                .map_or(line.len(), |length| start + length);
            if let Ok(symbol) = rustc_demangle::try_demangle(&line[start..end]) {
                writeln!(output, "{}{symbol:#}{}", &line[..start], &line[end..])?;
                continue;
            }
        }
        writeln!(output, "{line}")?;
    }
    output.flush()
}
