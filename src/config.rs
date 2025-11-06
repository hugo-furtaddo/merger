use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub output: PathBuf,
    pub inputs: Vec<PathBuf>,
    pub ext: String,
    pub recursive: bool,
    pub chunk_lines: usize,
    pub temp_dir: Option<PathBuf>,
    pub quiet: bool,
}

impl Config {
    pub fn validated_chunk_lines(&self) -> usize {
        self.chunk_lines.max(1)
    }
}
