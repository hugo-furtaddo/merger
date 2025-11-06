mod chunker;
pub mod config;
mod lines;
mod merger;
pub mod progress;
mod scanner;
mod temp;

pub use config::Config;
pub use progress::{ProgressReporter, ProgressSink};

use anyhow::Result;
use std::path::PathBuf;

pub fn run(config: Config) -> Result<()> {
    let (files, temp_factory) = prepare(&config)?;
    let mut progress = progress::ProgressReporter::new(!config.quiet, files.len());
    execute_pipeline(&config, files, temp_factory, &mut progress)
}

pub fn run_with_progress(config: Config, progress: &mut dyn ProgressSink) -> Result<()> {
    let (files, temp_factory) = prepare(&config)?;
    execute_pipeline(&config, files, temp_factory, progress)
}

fn prepare(config: &Config) -> Result<(Vec<PathBuf>, temp::TempFileFactory)> {
    let files = scanner::collect_input_files(config)?;
    let temp_factory = temp::TempFileFactory::new(config.temp_dir.as_deref(), &config.output)?;
    Ok((files, temp_factory))
}

fn execute_pipeline(
    config: &Config,
    files: Vec<PathBuf>,
    temp_factory: temp::TempFileFactory,
    progress: &mut dyn ProgressSink,
) -> Result<()> {
    let chunk_builder = chunker::ChunkBuilder::new(config.validated_chunk_lines(), &temp_factory);
    let temp_files = chunk_builder.build(&files, progress)?;
    if !temp_files.is_empty() {
        progress.start_merge(temp_files.len());
    }

    merger::merge_chunks(temp_files, &config.output, &temp_factory, progress)?;
    progress.finish(&config.output);
    Ok(())
}
