// Incremental base64 codec for bastion-relayed transfers.
//
// Bastion middleboxes inspect PTY output for content structure; a
// line-wrapped base64 stream (safe alphabet, periodicity broken every 76
// chars) is the only payload form empirically guaranteed to pass, so the
// jumpserver shell-copy transport encodes ALL payloads as base64:
//
//   download: `base64 <path>` (or `tail -c +N <path> | base64` to resume)
//   upload:   feeder encodes -> `head -c <wire_len> | base64 -d > tmp`
//
// The codec below is streaming: chunks may split at ANY byte boundary and
// contain `\r`/`\n` line breaks. The decoder stops consuming at the first
// non-alphabet character (the UUID end sentinel starts with `_`, which is
// outside the alphabet, giving a natural payload/marker boundary) and errors
// on short/overlong payloads instead of passing corruption silently.

use anyhow::{Result, bail};

/// Wire length (bytes on the PTY) of a fresh base64 stream for `decoded`
/// source bytes: `ceil(decoded/3)*4` alphabet chars, wrapped at 76 chars per
/// line with every line (including the last) newline-terminated.
pub(crate) fn wire_len_for(decoded: u64) -> u64 {
    let chars = decoded.div_ceil(3) * 4;
    let lines = chars.div_ceil(76);
    chars + lines
}

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

/// Streaming whitespace-tolerant base64 decoder with an expected payload
/// length. `feed` may be called with arbitrary chunk boundaries; it returns
/// newly decoded bytes and remembers carry state. Consumption stops
/// permanently at the first non-alphabet, non-whitespace byte.
pub(crate) struct B64Decoder {
    expected: u64,
    decoded: u64,
    quad: [u8; 4],
    quad_len: usize,
    /// Once true, no further input is consumed (end sentinel reached or
    /// padding seen).
    stopped: bool,
}

impl B64Decoder {
    pub(crate) fn new(expected: u64) -> Self {
        Self {
            expected,
            decoded: 0,
            quad: [0; 4],
            quad_len: 0,
            stopped: false,
        }
    }

    pub(crate) fn decoded(&self) -> u64 {
        self.decoded
    }

    /// Validate the final state after the stream ended. Ok only when exactly
    /// `expected` bytes were decoded.
    pub(crate) fn finish(&self) -> Result<()> {
        if self.decoded != self.expected {
            bail!(
                "base64 payload length mismatch: expected {} decoded bytes, got {}",
                self.expected,
                self.decoded
            );
        }
        Ok(())
    }

    /// Feed one wire chunk; returns bytes decoded from this chunk. Skips
    /// `\r` and `\n`. Stops consuming at any other non-alphabet byte. Errors
    /// when decoding past `expected` or on invalid padding placement.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        for &b in chunk {
            if self.stopped {
                break;
            }
            match b {
                b'\r' | b'\n' => continue,
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' => {
                    self.quad[self.quad_len] = b;
                    self.quad_len += 1;
                    if self.quad_len == 4 {
                        out.extend_from_slice(&decode_quad(&self.quad)?);
                        self.decoded += 3;
                        self.quad_len = 0;
                        if self.decoded > self.expected {
                            bail!(
                                "base64 payload larger than expected ({} > {})",
                                self.decoded,
                                self.expected
                            );
                        }
                    }
                }
                b'=' => {
                    // Padding: only valid completing a quad; ends the stream.
                    if self.quad_len < 2 {
                        bail!("invalid base64 padding position");
                    }
                    while self.quad_len < 4 {
                        self.quad[self.quad_len] = b'=';
                        self.quad_len += 1;
                    }
                    let bytes = decode_quad(&self.quad)?;
                    self.decoded += bytes.len() as u64;
                    out.extend_from_slice(&bytes);
                    self.quad_len = 0;
                    if self.decoded > self.expected {
                        bail!(
                            "base64 payload larger than expected ({} > {})",
                            self.decoded,
                            self.expected
                        );
                    }
                    self.stopped = true;
                }
                _ => {
                    // Not part of the payload — the end sentinel or prompt.
                    self.stopped = true;
                }
            }
        }
        Ok(out)
    }
}

fn decode_quad(quad: &[u8; 4]) -> Result<Vec<u8>> {
    let v = |c: u8| -> Result<u32> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => bail!("invalid base64 character in quad"),
        }
    };
    let pad = quad.iter().filter(|&&c| c == b'=').count();
    let a = v(quad[0])?;
    let b = v(quad[1])?;
    let triple = match pad {
        0 => {
            let c = v(quad[2])?;
            let d = v(quad[3])?;
            [
                ((a << 2) | (b >> 4)) as u8,
                ((b << 4) | (c >> 2)) as u8,
                ((c << 6) | d) as u8,
            ]
        }
        1 => {
            if quad[3] != b'=' {
                bail!("invalid base64 padding order");
            }
            let c = v(quad[2])?;
            [((a << 2) | (b >> 4)) as u8, ((b << 4) | (c >> 2)) as u8, 0]
        }
        2 => [((a << 2) | (b >> 4)) as u8, 0, 0],
        _ => bail!("invalid base64 quad padding"),
    };
    let len = 3 - pad;
    Ok(triple[..len].to_vec())
}

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

/// Streaming base64 encoder emitting exactly the `base64(1)` MIME layout:
/// 76 alphabet chars per line, every line newline-terminated. Input is
/// consumed in 57-byte groups (57 -> 76 chars) so lines stay aligned for
/// arbitrary chunking; `finish` pads the tail and emits the final newline.
pub(crate) struct B64Encoder {
    pending: Vec<u8>,
    encoded_total: u64,
}

/// 57 decoded bytes encode to exactly one 76-char line.
const GROUP: usize = 57;

impl B64Encoder {
    pub(crate) fn new() -> Self {
        Self {
            pending: Vec::new(),
            encoded_total: 0,
        }
    }

    pub(crate) fn encoded_total(&self) -> u64 {
        self.encoded_total
    }

    pub(crate) fn encode(&mut self, data: &[u8]) -> Vec<u8> {
        self.pending.extend_from_slice(data);
        let groups = self.pending.len() / GROUP;
        let mut out = Vec::with_capacity(groups * 77);
        for _ in 0..groups {
            let group: Vec<u8> = self.pending.drain(..GROUP).collect();
            out.extend_from_slice(&encode_group(&group));
            out.push(b'\n');
            self.encoded_total += 77;
        }
        out
    }

    /// Pad and flush the remainder; returns the final wire bytes (possibly
    /// empty for a zero-byte payload).
    pub(crate) fn finish(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        if !self.pending.is_empty() {
            let group: Vec<u8> = self.pending.drain(..).collect();
            out.extend_from_slice(&encode_group(&group));
            out.push(b'\n');
            self.encoded_total += out.len() as u64;
        }
        out
    }
}

fn encode_group(group: &[u8]) -> Vec<u8> {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as ENGINE;
    ENGINE.encode(group).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(data: &[u8], expected: u64) -> Result<Vec<u8>> {
        let mut d = B64Decoder::new(expected);
        let mut out = Vec::new();
        out.extend(d.feed(data)?);
        d.finish()?;
        Ok(out)
    }

    #[test]
    fn roundtrip_full_stream() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i * 31 % 251) as u8).collect();
        let mut enc = B64Encoder::new();
        let mut wire = enc.encode(&data);
        wire.extend(enc.finish());
        assert_eq!(wire.len() as u64, wire_len_for(data.len() as u64));
        assert_eq!(decode_all(&wire, data.len() as u64).unwrap(), data);
    }

    #[test]
    fn wire_is_valid_mime_base64_for_system_decoder() {
        // Contract: our wire layout (76-char lines, newline-terminated) must
        // decode byte-exact through the system `base64 -d` (what the remote
        // receiver runs). Exact wrap style of the local *encoder* differs
        // between BSD/GNU, so we verify against the decoder, not the encoder.
        let data: Vec<u8> = (0..50_000u32).map(|i| (i * 7 % 256) as u8).collect();
        let mut enc = B64Encoder::new();
        let mut wire = enc.encode(&data);
        wire.extend(enc.finish());
        assert_eq!(wire.len() as u64, wire_len_for(data.len() as u64));
        // structural: every line ≤76 chars, every line newline-terminated
        for line in wire.split(|&b| b == b'\n') {
            assert!(line.len() <= 76, "line longer than 76 chars");
        }
        assert_eq!(wire.last(), Some(&b'\n'));
        // decode with the system decoder
        let mut child = match std::process::Command::new("base64")
            .arg("-d")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(_) => return, // base64(1) absent — skip
        };
        use std::io::Write;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&wire)
            .expect("feed base64 -d");
        let out = child.wait_with_output().expect("wait base64 -d");
        assert!(out.status.success(), "system base64 -d rejected our wire");
        assert_eq!(out.stdout, data);
    }

    #[test]
    fn decoder_tolerates_arbitrary_chunk_splits() {
        let data: Vec<u8> = (0..9_999u32).map(|i| (i * 13 % 251) as u8).collect();
        let wire = {
            use base64::Engine as _;
            use base64::engine::general_purpose::STANDARD as ENGINE;
            ENGINE.encode(&data).into_bytes()
        };
        // split into 1-byte chunks with \r\n injected every 7 bytes
        let mut noisy = Vec::new();
        for (i, b) in wire.iter().enumerate() {
            if i % 7 == 0 && i > 0 {
                noisy.extend_from_slice(b"\r\n");
            }
            noisy.push(*b);
        }
        let mut d = B64Decoder::new(data.len() as u64);
        let mut out = Vec::new();
        for b in &noisy {
            out.extend(d.feed(std::slice::from_ref(b)).unwrap());
        }
        d.finish().unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn decoder_stops_at_end_sentinel() {
        // Payload followed by the raw-mode end sentinel (`__XHO_E_...:0\n`):
        // the leading `_` stops consumption; decoded count still validates.
        let data = b"hello world!!".to_vec(); // 13 bytes
        let wire = {
            use base64::Engine as _;
            use base64::engine::general_purpose::STANDARD as ENGINE;
            ENGINE.encode(&data).into_bytes()
        };
        let mut stream = wire.clone();
        stream.extend_from_slice(b"\n__XHO_E_abc123:0\n");
        let mut d = B64Decoder::new(data.len() as u64);
        let mut out = Vec::new();
        out.extend(d.feed(&stream).unwrap());
        d.finish().unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn decoder_rejects_short_payload() {
        let wire = b"QUJD"; // "ABC"
        let err = decode_all(wire, 10).unwrap_err();
        assert!(err.to_string().contains("mismatch"));
    }

    #[test]
    fn decoder_rejects_overlong_payload() {
        let wire = b"QUJDREFGR0g="; // "ABCDEFGH" (8 bytes)
        let err = decode_all(wire, 3).unwrap_err();
        assert!(err.to_string().contains("larger than expected"));
    }

    #[test]
    fn encoder_chunking_is_chunk_size_independent() {
        let data: Vec<u8> = (0..4_321u32).map(|i| (i * 11 % 241) as u8).collect();
        let mut one = B64Encoder::new();
        let mut w1 = one.encode(&data);
        w1.extend(one.finish());

        let mut piecemeal = B64Encoder::new();
        let mut w2 = Vec::new();
        for chunk in data.chunks(13) {
            w2.extend(piecemeal.encode(chunk));
        }
        w2.extend(piecemeal.finish());
        assert_eq!(w1, w2);
        assert_eq!(w1.len() as u64, wire_len_for(data.len() as u64));
    }

    #[test]
    fn wire_len_formula() {
        assert_eq!(wire_len_for(0), 0);
        assert_eq!(wire_len_for(1), 5); // 4 chars + 1 newline
        assert_eq!(wire_len_for(57), 77); // one full line
        assert_eq!(wire_len_for(58), 82); // 77 + (4 chars + \n)
        assert_eq!(wire_len_for(114), 154); // two full lines
    }
}
