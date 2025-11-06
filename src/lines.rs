use std::io::BufRead;

pub fn read_next_line<R: BufRead>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    let bytes_read = reader.read_until(b'\n', &mut buf)?;

    if bytes_read == 0 {
        return Ok(None);
    }

    trim_line_break(&mut buf);
    Ok(Some(buf))
}

pub fn trim_line_break(line: &mut Vec<u8>) {
    if let Some(b'\n') = line.last().copied() {
        line.pop();
        if let Some(b'\r') = line.last().copied() {
            line.pop();
        }
    } else if matches!(line.last().copied(), Some(b'\r')) {
        line.pop();
    }
}
