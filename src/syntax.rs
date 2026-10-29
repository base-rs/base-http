// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#[inline]
pub(crate) fn is_token(value: &[u8]) -> bool {
    // Nearly every name is letters, digits and `-`. This check has no branch,
    // so the compiler vectorizes it; anything else takes the full check.
    let usual = value.iter().fold(true, |usual, byte| {
        let letter = (byte | 0x20).wrapping_sub(b'a') < 26;
        let digit = byte.wrapping_sub(b'0') < 10;
        usual & (letter | digit | (*byte == b'-'))
    });
    !value.is_empty()
        && (usual
            || value.iter().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#'
                            | b'$'
                            | b'%'
                            | b'&'
                            | b'\''
                            | b'*'
                            | b'+'
                            | b'-'
                            | b'.'
                            | b'^'
                            | b'_'
                            | b'`'
                            | b'|'
                            | b'~'
                    )
            }))
}

pub(crate) fn trim_ows(mut value: &[u8]) -> &[u8] {
    while matches!(value.first(), Some(b' ' | b'\t')) {
        value = &value[1..];
    }
    while matches!(value.last(), Some(b' ' | b'\t')) {
        value = &value[..value.len() - 1];
    }
    value
}

#[cfg(test)]
mod tests {
    use super::is_token;

    #[test]
    fn tokens_are_recognized_with_and_without_the_fast_path() {
        let delimiters = b"\"(),/:;<=>?@[\\]{}";
        for byte in 0..=255u8 {
            let token = byte.is_ascii_graphic() && !delimiters.contains(&byte);
            for length in [1, 15, 16, 17, 40] {
                for position in 0..length {
                    let mut name = vec![b'a'; length];
                    name[position] = byte;
                    assert_eq!(
                        is_token(&name),
                        token,
                        "{byte:#x} at {position} of {length}"
                    );
                }
            }
        }
        assert!(!is_token(b""));
    }
}
