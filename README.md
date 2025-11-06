# ULP Merge

Ferramenta em Rust para mesclar grandes listas de texto (`.txt` por padrão), removendo duplicados e oferecendo feedback em tempo real. O projeto expõe:

- **CLI** (`ulp_merge`) para automação em linha de comando.
- **GUI** (`ulp_merge_gui`) construída com `eframe/egui`, útil para usuários que preferem selecionar arquivos e acompanhar o progresso visualmente.

## Requisitos

- [Rust](https://www.rust-lang.org/) 1.76+ (toolchain estável com `cargo`).
- Para a GUI, é necessário que o sistema tenha as bibliotecas padrão de janelas (no Linux: Wayland ou X11; no Windows/macOS, apenas o toolkit nativo).

## Estrutura do projeto

- `src/lib.rs`: pipeline principal (`run`/`run_with_progress`).
- `src/main.rs`: binário CLI minimalista.
- `src/bin/ulp_merge_gui.rs`: interface gráfica.
- `src/chunker.rs`, `merger.rs`, `scanner.rs`, etc.: módulos especializados e testados separadamente.

## Como compilar

```bash
# Build padrão (debug)
cargo build

# Build otimizado para distribuição
cargo build --release
```

Os executáveis resultantes ficam em `target/debug/` ou `target/release/`:

- `target/.../ulp_merge`
- `target/.../ulp_merge_gui`

## Executando a CLI

Listar ajuda e opções:

```bash
cargo run --bin ulp_merge -- --help
```

Exemplo prático (mescla todos os `.txt` sob `dados/` e joga em `resultado.txt`):

```bash
cargo run --bin ulp_merge -- \
  --output resultado.txt \
  --extension txt \
  --recursive \
  dados/
```

Parâmetros importantes:

- `--chunk-lines <N>` controla quantas linhas são acumuladas antes de criar um arquivo temporário (default: 1_000_000).
- `--temp-dir <DIR>` define manualmente onde ficam os temporários.
- `--quiet` desativa logs no stderr.

## Executando a GUI

```bash
cargo run --bin ulp_merge_gui
```

No aplicativo, use os botões “Adicionar arquivo/pasta” para escolher entradas, selecione o arquivo de saída e clique em **Processar**. A seção de logs mostra o andamento (arquivos processados, progresso do merge e caminho final gerado).

## Testes

```bash
cargo test
```

Os testes cobrem scanner, chunker, merger e fábrica de temporários. Execute-os sempre que fizer modificações ou antes de distribuir binários.

## Dicas adicionais

- Para instalar o binário CLI no sistema, use `cargo install --path .`.
- No Linux, certifique-se de ter as dependências de desenvolvimento de X11/Wayland (por exemplo `libxcb`, `libwayland-dev`) para compilar a GUI da `eframe`.
