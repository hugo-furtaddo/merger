use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tempfile::{Builder, NamedTempFile};
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(
    name = "ulp-merge",
    version,
    about = "Mescla listas ULP em um único arquivo, removendo linhas duplicadas.",
    arg_required_else_help = true
)]
struct Cli {
    #[arg(
        short,
        long,
        value_name = "ARQUIVO",
        help = "Arquivo de saída que receberá os dados mesclados"
    )]
    output: PathBuf,

    #[arg(
        value_name = "CAMINHO",
        help = "Arquivos ou diretórios de entrada a serem processados",
        required = true
    )]
    inputs: Vec<PathBuf>,

    #[arg(
        short = 'e',
        long = "extension",
        alias = "ext",
        default_value = "txt",
        value_name = "EXT",
        help = "Extensão usada para filtrar os arquivos de entrada"
    )]
    ext: String,

    #[arg(
        short,
        long,
        help = "Percorre diretórios recursivamente em busca de arquivos"
    )]
    recursive: bool,

    #[arg(
        long,
        default_value_t = 1_000_000,
        value_name = "LINHAS",
        help = "Quantidade de linhas por chunk antes de enviar para o merge"
    )]
    chunk_lines: usize,

    #[arg(
        long = "temp-dir",
        value_name = "DIR",
        help = "Diretório usado para armazenar arquivos temporários"
    )]
    temp_dir: Option<PathBuf>,

    #[arg(
        long = "quiet",
        help = "Suprime mensagens de progresso",
        action = clap::ArgAction::SetTrue
    )]
    quiet: bool,
}

struct TempFileFactory {
    primary: PathBuf,
    fallback: Option<PathBuf>,
}

impl TempFileFactory {
    fn new(preferred: Option<&Path>, output: &Path) -> Result<Self> {
        let primary = preferred.map(|dir| dir.to_path_buf()).unwrap_or_else(|| {
            output
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."))
        });

        if !primary.exists() {
            fs::create_dir_all(&primary).with_context(|| {
                format!("Não foi possível criar diretório temporário {:?}", primary)
            })?;
        }

        let fallback = if preferred.is_none() {
            let system_temp = std::env::temp_dir();
            if system_temp != primary {
                Some(system_temp)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(ref dir) = fallback {
            if !dir.exists() {
                let _ = fs::create_dir_all(dir);
            }
        }

        Ok(Self { primary, fallback })
    }

    fn create(&self) -> Result<NamedTempFile> {
        match Self::create_in(&self.primary) {
            Ok(file) => Ok(file),
            Err(primary_err) => {
                if let Some(ref fallback) = self.fallback {
                    Self::create_in(fallback).map_err(|fallback_err| {
                        anyhow!(
                            "Não foi possível criar arquivo temporário em {:?} ({}) nem em {:?} ({}). Use --temp-dir para apontar um local com espaço disponível.",
                            self.primary,
                            primary_err,
                            fallback,
                            fallback_err
                        )
                    })
                } else {
                    Err(anyhow!(
                        "Não foi possível criar arquivo temporário em {:?}: {}. Use --temp-dir para apontar um local com espaço disponível.",
                        self.primary,
                        primary_err
                    ))
                }
            }
        }
    }

    fn create_in(dir: &Path) -> std::io::Result<NamedTempFile> {
        Builder::new().prefix("ulp_merge_chunk").tempfile_in(dir)
    }
}

struct ProgressReporter {
    enabled: bool,
    total_files: usize,
    processed_files: usize,
    total_lines: u64,
    lines_since_tick: u64,
    last_emit: Instant,
    current_file: Option<String>,
}

impl ProgressReporter {
    fn new(enabled: bool, total_files: usize) -> Self {
        Self {
            enabled,
            total_files,
            processed_files: 0,
            total_lines: 0,
            lines_since_tick: 0,
            last_emit: Instant::now(),
            current_file: None,
        }
    }

    fn start_file(&mut self, path: &Path) {
        if !self.enabled {
            return;
        }
        self.current_file = Some(path.display().to_string());
        eprintln!(
            "[{}/{}] Processando {}",
            self.processed_files + 1,
            self.total_files.max(1),
            self.current_filename()
        );
        self.reset_tick();
    }

    fn on_line(&mut self) {
        if !self.enabled {
            return;
        }

        self.total_lines += 1;
        self.lines_since_tick += 1;

        if self.lines_since_tick >= 100_000 || self.last_emit.elapsed() >= Duration::from_secs(2) {
            eprintln!(
                "[{}/{}] {} — {} linhas lidas",
                self.processed_files + 1,
                self.total_files.max(1),
                self.current_filename(),
                self.total_lines
            );
            self.reset_tick();
        }
    }

    fn finish_file(&mut self, path: &Path) {
        if !self.enabled {
            return;
        }
        self.processed_files += 1;
        eprintln!(
            "[{}/{}] Concluído {}",
            self.processed_files,
            self.total_files.max(1),
            path.display()
        );
        self.reset_tick();
        self.current_file = None;
    }

    fn start_merge(&mut self, temp_count: usize) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "Iniciando etapa de merge com {} arquivo(s) temporário(s)",
            temp_count
        );
        self.reset_tick();
    }

    fn merge_round(&mut self, remaining: usize) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "Merge intermediário concluído. Restam {} arquivos temporários.",
            remaining
        );
        self.reset_tick();
    }

    fn finish(&mut self, output: &Path) {
        if !self.enabled {
            return;
        }
        eprintln!(
            "Processamento finalizado. Total de arquivos processados: {}. Linhas lidas: {}. Resultado salvo em {}",
            self.processed_files,
            self.total_lines,
            output.display()
        );
    }

    fn current_filename(&self) -> &str {
        self.current_file
            .as_deref()
            .unwrap_or("Arquivo desconhecido")
    }

    fn reset_tick(&mut self) {
        self.lines_since_tick = 0;
        self.last_emit = Instant::now();
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let files = collect_input_files(&cli)?;
    let temp_factory = TempFileFactory::new(cli.temp_dir.as_deref(), &cli.output)?;
    let mut progress = ProgressReporter::new(!cli.quiet, files.len());

    let temp_files = build_chunks(&files, cli.chunk_lines, &temp_factory, &mut progress)?;
    if !temp_files.is_empty() {
        progress.start_merge(temp_files.len());
    }
    merge_chunks(temp_files, &cli.output, &temp_factory, &mut progress)?;
    progress.finish(&cli.output);

    Ok(())
}

fn collect_input_files(cli: &Cli) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for input in &cli.inputs {
        if input.is_dir() {
            if cli.recursive {
                for entry in WalkDir::new(input) {
                    let entry = entry
                        .with_context(|| format!("Falha ao percorrer diretório {:?}", input))?;
                    let path = entry.path();
                    if path.is_file()
                        && has_matching_ext(path, &cli.ext)
                        && !same_file(path, &cli.output)
                    {
                        files.push(path.to_path_buf());
                    }
                }
            } else {
                let dir_iter = fs::read_dir(input)
                    .with_context(|| format!("Falha ao ler diretório {:?}", input))?;
                for entry in dir_iter {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file()
                        && has_matching_ext(&path, &cli.ext)
                        && !same_file(&path, &cli.output)
                    {
                        files.push(path);
                    }
                }
            }
        } else if input.is_file() {
            if has_matching_ext(input, &cli.ext) && !same_file(input, &cli.output) {
                files.push(input.to_path_buf());
            }
        } else {
            return Err(anyhow!("Caminho inválido: {:?}", input));
        }
    }

    if files.is_empty() {
        return Err(anyhow!(
            "Nenhum arquivo de entrada encontrado com a extensão informada"
        ));
    }

    files.sort();
    Ok(files)
}

fn has_matching_ext(path: &Path, ext: &str) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some(e) => e.eq_ignore_ascii_case(ext),
        None => false,
    }
}

fn same_file(a: &Path, b: &Path) -> bool {
    let ca = fs::canonicalize(a).unwrap_or_else(|_| a.to_path_buf());
    let cb = fs::canonicalize(b).unwrap_or_else(|_| b.to_path_buf());
    ca == cb
}

fn build_chunks(
    files: &[PathBuf],
    max_lines: usize,
    temp_factory: &TempFileFactory,
    progress: &mut ProgressReporter,
) -> Result<Vec<NamedTempFile>> {
    let mut temp_files: Vec<NamedTempFile> = Vec::new();
    let mut chunk: Vec<Vec<u8>> = Vec::with_capacity(max_lines.min(100_000));
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

            if count >= max_lines {
                flush_chunk(&mut chunk, &mut temp_files, temp_factory)?;
                count = 0;
            }
        }

        progress.finish_file(path);
    }

    if !chunk.is_empty() {
        flush_chunk(&mut chunk, &mut temp_files, temp_factory)?;
    }

    Ok(temp_files)
}

fn flush_chunk(
    chunk: &mut Vec<Vec<u8>>,
    temp_files: &mut Vec<NamedTempFile>,
    temp_factory: &TempFileFactory,
) -> Result<()> {
    if chunk.is_empty() {
        return Ok(());
    }

    chunk.sort_unstable();
    chunk.dedup();

    let mut tmp = temp_factory
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

const MAX_OPEN_MERGE_FILES: usize = 64;

fn merge_chunks(
    mut temp_files: Vec<NamedTempFile>,
    output: &Path,
    temp_factory: &TempFileFactory,
    progress: &mut ProgressReporter,
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
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

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

fn read_next_line<R: BufRead>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    let bytes_read = reader.read_until(b'\n', &mut buf)?;

    if bytes_read == 0 {
        return Ok(None);
    }

    trim_line_break(&mut buf);
    Ok(Some(buf))
}

fn trim_line_break(line: &mut Vec<u8>) {
    if let Some(b'\n') = line.last().copied() {
        line.pop();
        if let Some(b'\r') = line.last().copied() {
            line.pop();
        }
    } else if matches!(line.last().copied(), Some(b'\r')) {
        line.pop();
    }
}
