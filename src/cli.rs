use clap::Parser;
use std::path::PathBuf;
use ulp_merge::Config;

#[derive(Parser, Debug)]
#[command(
    name = "ulp-merge",
    version,
    about = "Mescla listas ULP em um único arquivo, removendo linhas duplicadas.",
    arg_required_else_help = true
)]
pub struct Cli {
    #[arg(
        short,
        long,
        value_name = "ARQUIVO",
        help = "Arquivo de saída que receberá os dados mesclados"
    )]
    pub output: PathBuf,

    #[arg(
        value_name = "CAMINHO",
        help = "Arquivos ou diretórios de entrada a serem processados",
        required = true
    )]
    pub inputs: Vec<PathBuf>,

    #[arg(
        short = 'e',
        long = "extension",
        alias = "ext",
        default_value = "txt",
        value_name = "EXT",
        help = "Extensão usada para filtrar os arquivos de entrada"
    )]
    pub ext: String,

    #[arg(
        short,
        long,
        help = "Percorre diretórios recursivamente em busca de arquivos"
    )]
    pub recursive: bool,

    #[arg(
        long,
        default_value_t = 1_000_000,
        value_name = "LINHAS",
        help = "Quantidade de linhas por chunk antes de enviar para o merge"
    )]
    pub chunk_lines: usize,

    #[arg(
        long = "temp-dir",
        value_name = "DIR",
        help = "Diretório usado para armazenar arquivos temporários"
    )]
    pub temp_dir: Option<PathBuf>,

    #[arg(
        long = "quiet",
        help = "Suprime mensagens de progresso",
        action = clap::ArgAction::SetTrue
    )]
    pub quiet: bool,
}

impl Cli {
    pub fn into_config(self) -> Config {
        Config {
            output: self.output,
            inputs: self.inputs,
            ext: self.ext,
            recursive: self.recursive,
            chunk_lines: self.chunk_lines,
            temp_dir: self.temp_dir,
            quiet: self.quiet,
        }
    }
}
