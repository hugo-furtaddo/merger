use eframe::egui;
use poll_promise::Promise;
use rfd::FileDialog;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use ulp_merge::progress::ProgressSink;
use ulp_merge::Config;

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "ULP Merge",
        options,
        Box::new(|_cc| Box::new(MergeGui::default())),
    )
}

struct MergeGui {
    inputs: Vec<String>,
    output: String,
    ext: String,
    recursive: bool,
    chunk_lines: String,
    temp_dir: String,
    quiet: bool,
    status: String,
    logs: Vec<String>,
    processing: bool,
    receiver: Option<Receiver<WorkerMessage>>,
    file_dialog: Option<Promise<Option<Vec<PathBuf>>>>,
    folder_dialog: Option<Promise<Option<PathBuf>>>,
    save_dialog: Option<Promise<Option<PathBuf>>>,
}

impl Default for MergeGui {
    fn default() -> Self {
        Self {
            inputs: Vec::new(),
            output: String::new(),
            ext: "txt".into(),
            recursive: false,
            chunk_lines: "1000000".into(),
            temp_dir: String::new(),
            quiet: false,
            status: "Pronto.".into(),
            logs: Vec::new(),
            processing: false,
            receiver: None,
            file_dialog: None,
            folder_dialog: None,
            save_dialog: None,
        }
    }
}

enum WorkerMessage {
    Log(String),
    Finished(Result<(), String>),
}

impl MergeGui {
    fn poll_worker(&mut self) {
        let mut disconnect = false;
        if let Some(rx) = &self.receiver {
            loop {
                match rx.try_recv() {
                    Ok(msg) => match msg {
                        WorkerMessage::Log(line) => {
                            self.logs.push(line);
                            const MAX_LOGS: usize = 500;
                            if self.logs.len() > MAX_LOGS {
                                let drain = self.logs.len() - MAX_LOGS;
                                self.logs.drain(0..drain);
                            }
                        }
                        WorkerMessage::Finished(result) => {
                            self.processing = false;
                            self.status = match result {
                                Ok(()) => "Processamento concluído com sucesso.".into(),
                                Err(err) => format!("Erro: {err}"),
                            };
                            disconnect = true;
                            break;
                        }
                    },
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        disconnect = true;
                        self.processing = false;
                        self.status = "Canal finalizado inesperadamente.".into();
                        break;
                    }
                }
            }
        }

        if disconnect {
            self.receiver = None;
        }
    }

    fn poll_dialogs(&mut self) {
        if let Some(result) = ready_result(&mut self.file_dialog) {
            if let Some(paths) = result {
                for path in paths {
                    self.add_input_path(path);
                }
            }
        }

        if let Some(result) = ready_result(&mut self.folder_dialog) {
            if let Some(path) = result {
                self.add_input_path(path);
            }
        }

        if let Some(result) = ready_result(&mut self.save_dialog) {
            if let Some(path) = result {
                self.output = path.display().to_string();
            }
        }
    }

    fn add_input_path(&mut self, path: PathBuf) {
        self.inputs.push(path.display().to_string());
    }

    fn build_config(&self) -> Result<Config, String> {
        let inputs: Vec<PathBuf> = self
            .inputs
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect();

        if inputs.is_empty() {
            return Err("Adicione pelo menos um arquivo ou diretório de entrada.".into());
        }

        let output = self.output.trim();
        if output.is_empty() {
            return Err("Informe o caminho do arquivo de saída.".into());
        }

        let chunk_lines = self
            .chunk_lines
            .trim()
            .parse::<usize>()
            .map_err(|_| "Valor inválido para linhas por chunk.".to_string())?;
        if chunk_lines == 0 {
            return Err("Linhas por chunk deve ser maior que zero.".into());
        }

        let ext = {
            let trimmed = self.ext.trim();
            if trimmed.is_empty() {
                "txt".to_string()
            } else {
                trimmed.to_string()
            }
        };

        let temp_dir = {
            let trimmed = self.temp_dir.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(PathBuf::from(trimmed))
            }
        };

        Ok(Config {
            output: PathBuf::from(output),
            inputs,
            ext,
            recursive: self.recursive,
            chunk_lines,
            temp_dir,
            quiet: self.quiet,
        })
    }

    fn start_processing(&mut self) {
        if self.processing {
            self.status = "Já existe um processamento em andamento.".into();
            return;
        }

        let config = match self.build_config() {
            Ok(cfg) => cfg,
            Err(err) => {
                self.status = err;
                return;
            }
        };

        let (tx, rx) = mpsc::channel();
        self.receiver = Some(rx);
        self.logs.clear();
        self.status = "Processando...".into();
        self.processing = true;

        thread::spawn(move || {
            let mut progress = GuiProgress::new(tx.clone());
            let result =
                ulp_merge::run_with_progress(config, &mut progress).map_err(|err| err.to_string());
            let _ = tx.send(WorkerMessage::Finished(result));
        });
    }
}

impl eframe::App for MergeGui {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_worker();
        self.poll_dialogs();

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("ULP Merge");
            ui.label("Configure os arquivos de entrada e saída para iniciar a mescla.");

            ui.separator();
            ui.label("Entradas:");

            ui.horizontal(|ui| {
                if ui.button("Adicionar arquivo").clicked() {
                    self.file_dialog = Some(spawn_file_dialog());
                }
                if ui.button("Adicionar pasta").clicked() {
                    self.folder_dialog = Some(spawn_folder_dialog());
                }
            });

            let mut remove_idx = None;
            for (idx, input) in self.inputs.iter_mut().enumerate() {
                ui.horizontal(|ui| {
                    ui.text_edit_singleline(input);
                    if ui.button("Remover").clicked() {
                        remove_idx = Some(idx);
                    }
                });
            }
            if let Some(idx) = remove_idx {
                self.inputs.remove(idx);
            }

            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Saída:");
                ui.text_edit_singleline(&mut self.output);
                if ui.button("Escolher...").clicked() {
                    let (dir, file) = save_dialog_defaults(&self.output);
                    self.save_dialog = Some(spawn_save_dialog(dir, file));
                }
            });

            ui.horizontal(|ui| {
                ui.label("Extensão:");
                ui.text_edit_singleline(&mut self.ext);
                ui.label("Linhas por chunk:");
                ui.text_edit_singleline(&mut self.chunk_lines);
            });

            ui.horizontal(|ui| {
                ui.checkbox(&mut self.recursive, "Recursivo");
                ui.checkbox(&mut self.quiet, "Modo silencioso");
            });

            ui.horizontal(|ui| {
                ui.label("Diretório temporário:");
                ui.text_edit_singleline(&mut self.temp_dir);
            });

            ui.separator();
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!self.processing, egui::Button::new("Processar"))
                    .clicked()
                {
                    self.start_processing();
                }
                if ui.button("Limpar logs").clicked() {
                    self.logs.clear();
                }
            });

            ui.separator();
            ui.label(format!("Status: {}", self.status));

            egui::CollapsingHeader::new("Logs")
                .default_open(true)
                .show(ui, |ui| {
                    egui::ScrollArea::vertical()
                        .max_height(200.0)
                        .show(ui, |ui| {
                            for line in &self.logs {
                                ui.label(line);
                            }
                        });
                });
        });

        ctx.request_repaint_after(std::time::Duration::from_millis(100));
    }
}

fn ready_result<T: Clone + Send + 'static>(slot: &mut Option<Promise<T>>) -> Option<T> {
    let ready = slot.as_ref().and_then(|promise| promise.ready().cloned());
    if ready.is_some() {
        *slot = None;
    }
    ready
}

fn spawn_file_dialog() -> Promise<Option<Vec<PathBuf>>> {
    Promise::spawn_thread("pick_files_dialog", || FileDialog::new().pick_files())
}

fn spawn_folder_dialog() -> Promise<Option<PathBuf>> {
    Promise::spawn_thread("pick_folder_dialog", || FileDialog::new().pick_folder())
}

fn spawn_save_dialog(dir: Option<PathBuf>, file: Option<String>) -> Promise<Option<PathBuf>> {
    Promise::spawn_thread("save_dialog", move || {
        let mut dialog = FileDialog::new();
        if let Some(dir) = dir.as_ref() {
            dialog = dialog.set_directory(dir);
        } else if let Ok(current_dir) = std::env::current_dir() {
            dialog = dialog.set_directory(current_dir);
        }
        if let Some(file) = file.as_ref() {
            dialog = dialog.set_file_name(file);
        }
        dialog.save_file()
    })
}

fn save_dialog_defaults(output: &str) -> (Option<PathBuf>, Option<String>) {
    let trimmed = output.trim();
    if trimmed.is_empty() {
        return (None, None);
    }
    let path = PathBuf::from(trimmed);
    let dir = path
        .parent()
        .map(|p| p.to_path_buf())
        .filter(|p| !p.as_os_str().is_empty());
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|s| s.to_string());
    (dir, file)
}

struct GuiProgress {
    tx: Sender<WorkerMessage>,
    total_lines: u64,
}

impl GuiProgress {
    fn new(tx: Sender<WorkerMessage>) -> Self {
        Self { tx, total_lines: 0 }
    }

    fn log(&self, msg: impl Into<String>) {
        let _ = self.tx.send(WorkerMessage::Log(msg.into()));
    }
}

impl ProgressSink for GuiProgress {
    fn start_file(&mut self, path: &std::path::Path) {
        self.log(format!("Processando {}", path.display()));
    }

    fn on_line(&mut self) {
        self.total_lines += 1;
        if self.total_lines % 100_000 == 0 {
            self.log(format!("Linhas lidas: {}", self.total_lines));
        }
    }

    fn finish_file(&mut self, path: &std::path::Path) {
        self.log(format!("Concluído {}", path.display()));
    }

    fn start_merge(&mut self, temp_count: usize) {
        self.log(format!(
            "Iniciando merge com {} arquivo(s) temporário(s)",
            temp_count
        ));
    }

    fn merge_round(&mut self, remaining: usize) {
        self.log(format!(
            "Merge intermediário concluído. Restam {} arquivos temporários.",
            remaining
        ));
    }

    fn finish(&mut self, output: &std::path::Path) {
        self.log(format!("Resultado salvo em {}", output.display()));
    }
}
