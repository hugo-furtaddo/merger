use crate::lines::read_next_line;
use crate::progress::ProgressSink;
use crate::temp::TempFileFactory;
use anyhow::{Context, Result};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use tempfile::NamedTempFile;

const MAX_OPEN_MERGE_FILES: usize = 64;

pub fn merge_chunks(
    mut temp_files: Vec<NamedTempFile>,
    output: &Path,
    temp_factory: &TempFileFactory,
    progress: &mut dyn ProgressSink,
) -> Result<()> {
    if temp_files.is_empty() {
        File::create(output)
            .with_context(|| format!("Não foi possível criar arquivo de saída {:?}", output))?;
        return Ok(());
    }

    while temp_files.len() > MAX_OPEN_MERGE_FILES {
        let mut next_round: Vec<NamedTempFile> = Vec::new();
        let mut group: Vec<NamedTempFile> = Vec::new();

        for temp_file in temp_files.into_iter() {
            group.push(temp_file);
            if group.len() == MAX_OPEN_MERGE_FILES {
                let merged = merge_group_into_temp(group, temp_factory)?;
                next_round.push(merged);
                group = Vec::new();
            }
        }

        if !group.is_empty() {
            if group.len() == 1 {
                next_round.push(group.pop().unwrap());
            } else {
                let merged = merge_group_into_temp(group, temp_factory)?;
                next_round.push(merged);
            }
        }

        progress.merge_round(next_round.len());
        temp_files = next_round;
    }

    let out_file = File::create(output)
        .with_context(|| format!("Não foi possível criar arquivo de saída {:?}", output))?;
    let mut writer = BufWriter::new(out_file);
    merge_into_writer(&temp_files, &mut writer)?;
    writer
        .flush()
        .context("Falha ao finalizar escrita do arquivo de saída")?;
    Ok(())
}

fn merge_group_into_temp(
    group: Vec<NamedTempFile>,
    temp_factory: &TempFileFactory,
) -> Result<NamedTempFile> {
    let mut tmp = temp_factory
        .create()
        .context("Não foi possível criar arquivo temporário para merge")?;
    {
        let mut writer = BufWriter::new(&mut tmp);
        merge_into_writer(&group, &mut writer)?;
        writer
            .flush()
            .context("Erro ao finalizar escrita de arquivo temporário de merge")?;
    }

    Ok(tmp)
}

fn merge_into_writer<W: Write>(sources: &[NamedTempFile], writer: &mut W) -> Result<()> {
    if sources.is_empty() {
        return Ok(());
    }

    let mut readers: Vec<BufReader<File>> = Vec::with_capacity(sources.len());
    for tmp in sources {
        let file = tmp
            .reopen()
            .context("Não foi possível reabrir arquivo temporário para leitura")?;
        readers.push(BufReader::new(file));
    }

    let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some(line) = read_next_line(reader).context("Erro ao ler de arquivo temporário")? {
            heap.push(Reverse((line, idx)));
        }
    }

    let mut last_written: Option<Vec<u8>> = None;
    while let Some(Reverse((line, idx))) = heap.pop() {
        let should_write = match &last_written {
            Some(last) => last != &line,
            None => true,
        };

        if should_write {
            writer
                .write_all(&line)
                .context("Erro ao escrever no destino de merge")?;
            writer
                .write_all(b"\n")
                .context("Erro ao escrever quebra de linha no destino de merge")?;
            last_written = Some(line.clone());
        }

        if let Some(next_line) =
            read_next_line(&mut readers[idx]).context("Erro ao ler de arquivo temporário")?
        {
            heap.push(Reverse((next_line, idx)));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::ProgressSink;
    use std::io::Write;
    use tempfile::tempdir;

    struct NoopProgress;
    impl ProgressSink for NoopProgress {}

    #[test]
    fn merges_and_deduplicates_all_chunks() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("merged.txt");
        let factory = TempFileFactory::new(Some(dir.path()), &output).unwrap();

        let mut tmp1 = factory.create().unwrap();
        {
            let mut writer = BufWriter::new(&mut tmp1);
            writer.write_all(b"a\nc\n").unwrap();
            writer.flush().unwrap();
        }

        let mut tmp2 = factory.create().unwrap();
        {
            let mut writer = BufWriter::new(&mut tmp2);
            writer.write_all(b"b\nc\n").unwrap();
            writer.flush().unwrap();
        }

        let mut progress = NoopProgress;
        merge_chunks(vec![tmp1, tmp2], &output, &factory, &mut progress).unwrap();
        let result = std::fs::read_to_string(&output).unwrap();
        assert_eq!(result, "a\nb\nc\n");
    }
}
