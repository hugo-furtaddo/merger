use anyhow::{anyhow, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::{Builder, NamedTempFile};

pub struct TempFileFactory {
    primary: PathBuf,
    fallback: Option<PathBuf>,
}

impl TempFileFactory {
    pub fn new(preferred: Option<&Path>, output: &Path) -> Result<Self> {
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

    pub fn create(&self) -> Result<NamedTempFile> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn creates_primary_directory_when_missing() {
        let dir = tempdir().unwrap();
        let custom = dir.path().join("custom_tmp");
        let output = dir.path().join("out.txt");
        assert!(!custom.exists());
        let factory = TempFileFactory::new(Some(&custom), &output).unwrap();
        assert!(custom.exists());
        factory.create().unwrap();
    }
}
