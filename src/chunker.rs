use crate::lines::read_next_line;
use crate::progress::ProgressSink;
use crate::temp::TempFileFactory;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use tempfile::NamedTempFile;

pub struct ChunkBuilder<'a> {
    max_lines: usize,
    temp_factory: &'a TempFileFactory,
}

impl<'a> ChunkBuilder<'a> {
    pub fn new(max_lines: usize, temp_factory: &'a TempFileFactory) -> Self {
        Self {
            max_lines,
            temp_factory,
        }
    }

    pub fn build(
        &self,
        files: &[PathBuf],
        progress: &mut dyn ProgressSink,
    ) -> Result<Vec<NamedTempFile>> {
        let mut temp_files: Vec<NamedTempFile> = Vec::new();
        let mut chunk: Vec<Vec<u8>> = Vec::with_capacity(self.max_lines.min(100_000));
        let mut count: usize = 0;

        for path in files {
            progress.start_file(path);

            let file =
                File::open(path).with_context(|| format!("Falha ao abrir arquivo {:?}", path))?;
            let mut reader = BufReader::new(file);

            while let Some(line) = read_next_line(&mut reader)
                .with_context(|| format!("Erro ao ler linha em {:?}", path))?
            {
                chunk.push(line);
                count += 1;
                progress.on_line();

                if count >= self.max_lines {
                    self.flush_chunk(&mut chunk, &mut temp_files)?;
                    count = 0;
                }
            }

            progress.finish_file(path);
        }

        if !chunk.is_empty() {
            self.flush_chunk(&mut chunk, &mut temp_files)?;
        }

        Ok(temp_files)
    }

    fn flush_chunk(
        &self,
        chunk: &mut Vec<Vec<u8>>,
        temp_files: &mut Vec<NamedTempFile>,
    ) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        chunk.sort_unstable();
        chunk.dedup();

        let mut tmp = self
            .temp_factory
            .create()
            .context("Não foi possível criar arquivo temporário")?;
        {
            let mut writer = BufWriter::new(&mut tmp);
            for line in chunk.iter() {
                writer
                    .write_all(line)
                    .context("Erro ao escrever em arquivo temporário")?;
                writer
                    .write_all(b"\n")
                    .context("Erro ao escrever quebra de linha em arquivo temporário")?;
            }
            writer
                .flush()
                .context("Erro ao finalizar escrita de arquivo temporário")?;
        }

        temp_files.push(tmp);
        chunk.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::ProgressSink;
    use std::io::Read;
    use tempfile::tempdir;

    struct NoopProgress;
    impl ProgressSink for NoopProgress {}

    #[test]
    fn splits_chunks_and_deduplicates_lines() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("input.txt");
        std::fs::write(&input, b"c\nb\na\na\n").unwrap();
        let config_output = dir.path().join("out.txt");
        let factory = TempFileFactory::new(Some(dir.path()), &config_output).unwrap();
        let builder = ChunkBuilder::new(2, &factory);
        let mut progress = NoopProgress;
        let chunks = builder.build(&[input], &mut progress).unwrap();
        assert_eq!(chunks.len(), 2);

        let mut contents = Vec::new();
        for tmp in chunks {
            let mut file = tmp.reopen().unwrap();
            let mut data = String::new();
            file.read_to_string(&mut data).unwrap();
            contents.push(data);
        }
        contents.sort();
        assert_eq!(contents[0], "a\n");
        assert_eq!(contents[1], "b\nc\n");
    }
}
