use std::path::Path;
use std::time::{Duration, Instant};

pub trait ProgressSink {
    fn start_file(&mut self, _path: &Path) {}
    fn on_line(&mut self) {}
    fn finish_file(&mut self, _path: &Path) {}
    fn start_merge(&mut self, _temp_count: usize) {}
    fn merge_round(&mut self, _remaining: usize) {}
    fn finish(&mut self, _output: &Path) {}
}

pub struct ProgressReporter {
    enabled: bool,
    total_files: usize,
    processed_files: usize,
    total_lines: u64,
    lines_since_tick: u64,
    last_emit: Instant,
    current_file: Option<String>,
}

impl ProgressReporter {
    pub fn new(enabled: bool, total_files: usize) -> Self {
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

impl ProgressSink for ProgressReporter {
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
}
