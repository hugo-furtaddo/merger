use crate::config::Config;
use anyhow::{anyhow, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub fn collect_input_files(config: &Config) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for input in &config.inputs {
        if input.is_dir() {
            if config.recursive {
                collect_recursive(input, &config.output, &config.ext, &mut files)?;
            } else {
                collect_shallow(input, &config.output, &config.ext, &mut files)?;
            }
        } else if input.is_file() {
            if has_matching_ext(input, &config.ext) && !same_file(input, &config.output) {
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

fn collect_recursive(input: &Path, output: &Path, ext: &str, acc: &mut Vec<PathBuf>) -> Result<()> {
    for entry in WalkDir::new(input) {
        let entry = entry.with_context(|| format!("Falha ao percorrer diretório {:?}", input))?;
        let path = entry.path();
        if path.is_file() && has_matching_ext(path, ext) && !same_file(path, output) {
            acc.push(path.to_path_buf());
        }
    }
    Ok(())
}

fn collect_shallow(input: &Path, output: &Path, ext: &str, acc: &mut Vec<PathBuf>) -> Result<()> {
    let dir_iter =
        fs::read_dir(input).with_context(|| format!("Falha ao ler diretório {:?}", input))?;
    for entry in dir_iter {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && has_matching_ext(&path, ext) && !same_file(&path, output) {
            acc.push(path);
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::fs::File;
    use tempfile::tempdir;

    fn build_config(inputs: Vec<PathBuf>, output: PathBuf, recursive: bool) -> Config {
        Config {
            output,
            inputs,
            ext: "txt".into(),
            recursive,
            chunk_lines: 10,
            temp_dir: None,
            quiet: true,
        }
    }

    #[test]
    fn collects_recursive_entries() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("nested");
        fs::create_dir_all(&sub).unwrap();
        let file_a = dir.path().join("a.txt");
        let file_b = sub.join("b.txt");
        let file_other = sub.join("c.csv");
        File::create(&file_a).unwrap();
        File::create(&file_b).unwrap();
        File::create(&file_other).unwrap();

        let config = build_config(
            vec![dir.path().to_path_buf()],
            dir.path().join("out.txt"),
            true,
        );
        let files = collect_input_files(&config).unwrap();
        assert_eq!(files, vec![file_a.clone(), file_b.clone()]);
    }

    #[test]
    fn skips_output_file_and_wrong_extension() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("data.txt");
        let output = input.clone();
        File::create(&input).unwrap();
        let config = build_config(vec![dir.path().to_path_buf()], output, false);
        let err = collect_input_files(&config).unwrap_err();
        assert!(format!("{err}").contains("Nenhum arquivo"));
    }
}
