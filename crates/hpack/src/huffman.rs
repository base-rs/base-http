// Released under the MIT License.
// Copyright, 2014-2015, by Kaoru Maeda.
// Copyright, 2015, by Ilya Grigorik.
// Copyright, 2015, by Tamir Duberstein.
// Copyright, 2016, by George Ulmer.
// Copyright, 2018-2026, by Samuel Williams.
// Copyright, 2022, by Daniel Morrison.
// Copyright, 2024, by Nathan Froyd.

//! The Huffman code of string literals, [RFC 7541 §5.2](https://www.rfc-editor.org/rfc/rfc7541#section-5.2) and [Appendix B](https://www.rfc-editor.org/rfc/rfc7541#appendix-B).
//!
//! Decoding follows `protocol-hpack`'s generated state machine, which consumes four bits at a time: each state is an internal node of the code tree, and each transition emits at most one symbol, because the shortest code has five bits. The machine is built from [`CODES`] at compile time rather than generated into source, so the table cannot drift from the code.

use crate::DecodeError;

/// The symbol that ends a string, which must never appear in one.
const END_OF_STRING: usize = 256;

/// The code and its length in bits for each symbol, RFC 7541 Appendix B.
const CODES: [(u32, u8); 257] = [
    (0x1ff8, 13),
    (0x7fffd8, 23),
    (0xfffffe2, 28),
    (0xfffffe3, 28),
    (0xfffffe4, 28),
    (0xfffffe5, 28),
    (0xfffffe6, 28),
    (0xfffffe7, 28),
    (0xfffffe8, 28),
    (0xffffea, 24),
    (0x3ffffffc, 30),
    (0xfffffe9, 28),
    (0xfffffea, 28),
    (0x3ffffffd, 30),
    (0xfffffeb, 28),
    (0xfffffec, 28),
    (0xfffffed, 28),
    (0xfffffee, 28),
    (0xfffffef, 28),
    (0xffffff0, 28),
    (0xffffff1, 28),
    (0xffffff2, 28),
    (0x3ffffffe, 30),
    (0xffffff3, 28),
    (0xffffff4, 28),
    (0xffffff5, 28),
    (0xffffff6, 28),
    (0xffffff7, 28),
    (0xffffff8, 28),
    (0xffffff9, 28),
    (0xffffffa, 28),
    (0xffffffb, 28),
    (0x14, 6),
    (0x3f8, 10),
    (0x3f9, 10),
    (0xffa, 12),
    (0x1ff9, 13),
    (0x15, 6),
    (0xf8, 8),
    (0x7fa, 11),
    (0x3fa, 10),
    (0x3fb, 10),
    (0xf9, 8),
    (0x7fb, 11),
    (0xfa, 8),
    (0x16, 6),
    (0x17, 6),
    (0x18, 6),
    (0x0, 5),
    (0x1, 5),
    (0x2, 5),
    (0x19, 6),
    (0x1a, 6),
    (0x1b, 6),
    (0x1c, 6),
    (0x1d, 6),
    (0x1e, 6),
    (0x1f, 6),
    (0x5c, 7),
    (0xfb, 8),
    (0x7ffc, 15),
    (0x20, 6),
    (0xffb, 12),
    (0x3fc, 10),
    (0x1ffa, 13),
    (0x21, 6),
    (0x5d, 7),
    (0x5e, 7),
    (0x5f, 7),
    (0x60, 7),
    (0x61, 7),
    (0x62, 7),
    (0x63, 7),
    (0x64, 7),
    (0x65, 7),
    (0x66, 7),
    (0x67, 7),
    (0x68, 7),
    (0x69, 7),
    (0x6a, 7),
    (0x6b, 7),
    (0x6c, 7),
    (0x6d, 7),
    (0x6e, 7),
    (0x6f, 7),
    (0x70, 7),
    (0x71, 7),
    (0x72, 7),
    (0xfc, 8),
    (0x73, 7),
    (0xfd, 8),
    (0x1ffb, 13),
    (0x7fff0, 19),
    (0x1ffc, 13),
    (0x3ffc, 14),
    (0x22, 6),
    (0x7ffd, 15),
    (0x3, 5),
    (0x23, 6),
    (0x4, 5),
    (0x24, 6),
    (0x5, 5),
    (0x25, 6),
    (0x26, 6),
    (0x27, 6),
    (0x6, 5),
    (0x74, 7),
    (0x75, 7),
    (0x28, 6),
    (0x29, 6),
    (0x2a, 6),
    (0x7, 5),
    (0x2b, 6),
    (0x76, 7),
    (0x2c, 6),
    (0x8, 5),
    (0x9, 5),
    (0x2d, 6),
    (0x77, 7),
    (0x78, 7),
    (0x79, 7),
    (0x7a, 7),
    (0x7b, 7),
    (0x7ffe, 15),
    (0x7fc, 11),
    (0x3ffd, 14),
    (0x1ffd, 13),
    (0xffffffc, 28),
    (0xfffe6, 20),
    (0x3fffd2, 22),
    (0xfffe7, 20),
    (0xfffe8, 20),
    (0x3fffd3, 22),
    (0x3fffd4, 22),
    (0x3fffd5, 22),
    (0x7fffd9, 23),
    (0x3fffd6, 22),
    (0x7fffda, 23),
    (0x7fffdb, 23),
    (0x7fffdc, 23),
    (0x7fffdd, 23),
    (0x7fffde, 23),
    (0xffffeb, 24),
    (0x7fffdf, 23),
    (0xffffec, 24),
    (0xffffed, 24),
    (0x3fffd7, 22),
    (0x7fffe0, 23),
    (0xffffee, 24),
    (0x7fffe1, 23),
    (0x7fffe2, 23),
    (0x7fffe3, 23),
    (0x7fffe4, 23),
    (0x1fffdc, 21),
    (0x3fffd8, 22),
    (0x7fffe5, 23),
    (0x3fffd9, 22),
    (0x7fffe6, 23),
    (0x7fffe7, 23),
    (0xffffef, 24),
    (0x3fffda, 22),
    (0x1fffdd, 21),
    (0xfffe9, 20),
    (0x3fffdb, 22),
    (0x3fffdc, 22),
    (0x7fffe8, 23),
    (0x7fffe9, 23),
    (0x1fffde, 21),
    (0x7fffea, 23),
    (0x3fffdd, 22),
    (0x3fffde, 22),
    (0xfffff0, 24),
    (0x1fffdf, 21),
    (0x3fffdf, 22),
    (0x7fffeb, 23),
    (0x7fffec, 23),
    (0x1fffe0, 21),
    (0x1fffe1, 21),
    (0x3fffe0, 22),
    (0x1fffe2, 21),
    (0x7fffed, 23),
    (0x3fffe1, 22),
    (0x7fffee, 23),
    (0x7fffef, 23),
    (0xfffea, 20),
    (0x3fffe2, 22),
    (0x3fffe3, 22),
    (0x3fffe4, 22),
    (0x7ffff0, 23),
    (0x3fffe5, 22),
    (0x3fffe6, 22),
    (0x7ffff1, 23),
    (0x3ffffe0, 26),
    (0x3ffffe1, 26),
    (0xfffeb, 20),
    (0x7fff1, 19),
    (0x3fffe7, 22),
    (0x7ffff2, 23),
    (0x3fffe8, 22),
    (0x1ffffec, 25),
    (0x3ffffe2, 26),
    (0x3ffffe3, 26),
    (0x3ffffe4, 26),
    (0x7ffffde, 27),
    (0x7ffffdf, 27),
    (0x3ffffe5, 26),
    (0xfffff1, 24),
    (0x1ffffed, 25),
    (0x7fff2, 19),
    (0x1fffe3, 21),
    (0x3ffffe6, 26),
    (0x7ffffe0, 27),
    (0x7ffffe1, 27),
    (0x3ffffe7, 26),
    (0x7ffffe2, 27),
    (0xfffff2, 24),
    (0x1fffe4, 21),
    (0x1fffe5, 21),
    (0x3ffffe8, 26),
    (0x3ffffe9, 26),
    (0xffffffd, 28),
    (0x7ffffe3, 27),
    (0x7ffffe4, 27),
    (0x7ffffe5, 27),
    (0xfffec, 20),
    (0xfffff3, 24),
    (0xfffed, 20),
    (0x1fffe6, 21),
    (0x3fffe9, 22),
    (0x1fffe7, 21),
    (0x1fffe8, 21),
    (0x7ffff3, 23),
    (0x3fffea, 22),
    (0x3fffeb, 22),
    (0x1ffffee, 25),
    (0x1ffffef, 25),
    (0xfffff4, 24),
    (0xfffff5, 24),
    (0x3ffffea, 26),
    (0x7ffff4, 23),
    (0x3ffffeb, 26),
    (0x7ffffe6, 27),
    (0x3ffffec, 26),
    (0x3ffffed, 26),
    (0x7ffffe7, 27),
    (0x7ffffe8, 27),
    (0x7ffffe9, 27),
    (0x7ffffea, 27),
    (0x7ffffeb, 27),
    (0xffffffe, 28),
    (0x7ffffec, 27),
    (0x7ffffed, 27),
    (0x7ffffee, 27),
    (0x7ffffef, 27),
    (0x7fffff0, 27),
    (0x3ffffee, 26),
    (0x3fffffff, 30),
];

/// The number of bytes `bytes` occupies once Huffman coded, including padding.
pub(crate) fn encoded_len(bytes: &[u8]) -> usize {
    let bits: usize = bytes.iter().map(|&byte| usize::from(CODES[usize::from(byte)].1)).sum();
    bits.div_ceil(8)
}

/// Appends the Huffman code of `bytes` to `output`, padded with the most significant bits of the end-of-string code to a whole byte.
pub(crate) fn encode(bytes: &[u8], output: &mut Vec<u8>) {
    // Only the low `pending` bits of the accumulator are unwritten: older bits are shifted out of it unread.
    let mut accumulator: u64 = 0;
    let mut pending: u32 = 0;
    for &byte in bytes {
        let (code, length) = CODES[usize::from(byte)];
        accumulator = (accumulator << length) | u64::from(code);
        pending += u32::from(length);
        while pending >= 8 {
            pending -= 8;
            output.push((accumulator >> pending) as u8);
        }
    }
    if pending > 0 {
        let padding = 8 - pending;
        output.push(((accumulator << padding) as u8) | ((1 << padding) - 1));
    }
}

/// Decodes the Huffman coded `input`, appending the result to `output`.
///
/// Fails with [`DecodeError::HuffmanEndOfString`] if the input codes the end-of-string symbol, and with [`DecodeError::HuffmanPadding`] if its final bits are not a prefix of at most seven bits of that symbol's code, as RFC 7541 §5.2 requires.
pub(crate) fn decode(input: &[u8], output: &mut Vec<u8>) -> Result<(), DecodeError> {
    // The shortest code has five bits, so the output is at most eight fifths of the input.
    output.reserve(input.len() / 5 * 8 + 8);
    let mut state = 0;
    for &byte in input {
        for nibble in [byte >> 4, byte & 0x0f] {
            let transition = MACHINE.transitions[state][usize::from(nibble)];
            if transition.flags & FAIL != 0 {
                return Err(DecodeError::HuffmanEndOfString);
            }
            if transition.flags & EMIT != 0 {
                output.push(transition.symbol);
            }
            state = usize::from(transition.state);
        }
    }
    if MACHINE.accepting[state] {
        Ok(())
    } else {
        Err(DecodeError::HuffmanPadding)
    }
}

/// The transition emits `symbol`.
const EMIT: u8 = 1;
/// The transition completes the end-of-string symbol.
const FAIL: u8 = 2;

#[derive(Clone, Copy)]
struct Transition {
    state: u8,
    symbol: u8,
    flags: u8,
}

struct Machine {
    transitions: [[Transition; 16]; 256],
    /// Whether the bits consumed since the last symbol are valid padding: at most seven bits, all ones.
    accepting: [bool; 256],
}

static MACHINE: Machine = machine();

/// Marks a child of the code tree that is a leaf, holding its symbol in the low bits.
const LEAF: u16 = 0x8000;

/// Builds the decoding machine from [`CODES`], as `protocol-hpack`'s generator does.
const fn machine() -> Machine {
    // The code tree's 256 internal nodes; node 0 is the root, so a child of 0 is absent.
    let mut children = [[0u16; 2]; 256];
    let mut nodes = 1;
    let mut symbol = 0;
    while symbol < CODES.len() {
        let (code, length) = CODES[symbol];
        let mut node = 0;
        let mut bit = length;
        while bit > 1 {
            bit -= 1;
            let branch = ((code >> bit) & 1) as usize;
            if children[node][branch] == 0 {
                children[node][branch] = nodes as u16;
                nodes += 1;
            }
            node = children[node][branch] as usize;
        }
        children[node][(code & 1) as usize] = LEAF | symbol as u16;
        symbol += 1;
    }

    let empty = Transition { state: 0, symbol: 0, flags: 0 };
    let mut transitions = [[empty; 16]; 256];
    let mut state = 0;
    while state < 256 {
        let mut nibble = 0;
        while nibble < 16 {
            let mut transition = empty;
            let mut node = state;
            let mut bit = 4;
            while bit > 0 {
                bit -= 1;
                let child = children[node][(nibble >> bit) & 1];
                if child & LEAF == 0 {
                    node = child as usize;
                } else {
                    let symbol = (child & !LEAF) as usize;
                    if symbol == END_OF_STRING {
                        transition.flags |= FAIL;
                    } else {
                        // Four bits complete at most one symbol, since the shortest code has five; the tests check the machine against a decoder that walks the tree bit by bit.
                        transition.flags |= EMIT;
                        transition.symbol = symbol as u8;
                    }
                    node = 0;
                }
            }
            transition.state = node as u8;
            transitions[state][nibble] = transition;
            nibble += 1;
        }
        state += 1;
    }

    // Valid padding is the root, after a symbol ends on a byte boundary, or up to seven one bits after it.
    let mut accepting = [false; 256];
    let mut node = 0;
    let mut ones = 0;
    loop {
        accepting[node] = true;
        if ones == 7 {
            break;
        }
        node = children[node][1] as usize;
        ones += 1;
    }

    Machine { transitions, accepting }
}

#[cfg(test)]
mod tests;
