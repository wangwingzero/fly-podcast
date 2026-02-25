use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Type of a setting field.
pub enum FieldType {
    Text { is_secret: bool, placeholder: &'static str },
    Toggle,
}

/// A setting field displayed in the settings UI.
pub struct SettingField {
    pub key: &'static str,
    pub label: &'static str,
    pub field_type: FieldType,
}

/// Settings groups for the podcast pipeline.
pub const SETTING_GROUPS: &[(&str, &[SettingField])] = &[
    ("LLM (剧本生成)", &[
        SettingField { key: "LLM_API_KEY",  label: "API Key",  field_type: FieldType::Text { is_secret: true,  placeholder: "sk-..." } },
        SettingField { key: "LLM_BASE_URL", label: "Base URL", field_type: FieldType::Text { is_secret: false, placeholder: "https://api.openai.com/v1/chat/completions" } },
        SettingField { key: "LLM_MODEL",    label: "Model",    field_type: FieldType::Text { is_secret: false, placeholder: "gpt-4o" } },
    ]),
    ("语音合成 (TTS)", &[
        SettingField { key: "TTS_ENABLE_DASHSCOPE", label: "启用付费 DashScope",  field_type: FieldType::Toggle },
        SettingField { key: "DASHSCOPE_API_KEY",    label: "DashScope API Key",   field_type: FieldType::Text { is_secret: true, placeholder: "sk-..." } },
        SettingField { key: "TTS_ENABLE_EDGE",      label: "启用 Edge TTS (备用)", field_type: FieldType::Toggle },
    ]),
    ("微信公众号", &[
        SettingField { key: "WECHAT_APP_ID",     label: "App ID",     field_type: FieldType::Text { is_secret: false, placeholder: "" } },
        SettingField { key: "WECHAT_APP_SECRET",  label: "App Secret", field_type: FieldType::Text { is_secret: true,  placeholder: "" } },
        SettingField { key: "WECHAT_PROXY",       label: "代理地址",    field_type: FieldType::Text { is_secret: false, placeholder: "http://127.0.0.1:7890" } },
    ]),
    ("R2 存储", &[
        SettingField { key: "R2_DOMAIN", label: "域名", field_type: FieldType::Text { is_secret: false, placeholder: "ccar.hudawang.cn" } },
    ]),
];

/// In-memory key-value store backed by .env file.
pub struct Settings {
    pub values: BTreeMap<String, String>,
    pub env_path: PathBuf,
    pub dirty: bool,
    /// Track which secret fields are being shown
    pub visible_secrets: std::collections::HashSet<String>,
}

impl Settings {
    /// Load settings from the project's .env file.
    pub fn load(project_root: &Path) -> Self {
        let env_path = project_root.join(".env");
        let values = if env_path.exists() {
            parse_env_file(&env_path)
        } else {
            BTreeMap::new()
        };
        Self {
            values,
            env_path,
            dirty: false,
            visible_secrets: std::collections::HashSet::new(),
        }
    }

    pub fn get(&self, key: &str) -> &str {
        self.values.get(key).map(|s| s.as_str()).unwrap_or("")
    }

    pub fn get_bool(&self, key: &str) -> bool {
        matches!(self.get(key).to_lowercase().as_str(), "true" | "1" | "yes")
    }

    pub fn set(&mut self, key: &str, value: String) {
        let old = self.values.get(key).cloned().unwrap_or_default();
        if old != value {
            self.values.insert(key.to_string(), value);
            self.dirty = true;
        }
    }

    pub fn set_bool(&mut self, key: &str, value: bool) {
        self.set(key, if value { "true" } else { "false" }.to_string());
    }

    /// Save settings back to the .env file, preserving comments and unknown keys.
    pub fn save(&mut self) -> Result<(), String> {
        let content = if self.env_path.exists() {
            std::fs::read_to_string(&self.env_path).unwrap_or_default()
        } else {
            String::new()
        };

        let mut output_lines: Vec<String> = Vec::new();
        let mut written_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                output_lines.push(line.to_string());
                continue;
            }
            if let Some(eq_pos) = trimmed.find('=') {
                let key = trimmed[..eq_pos].trim();
                if let Some(new_val) = self.values.get(key) {
                    output_lines.push(format!("{key}={new_val}"));
                    written_keys.insert(key.to_string());
                } else {
                    output_lines.push(line.to_string());
                }
            } else {
                output_lines.push(line.to_string());
            }
        }

        // Append any new keys not in the original file
        for (key, val) in &self.values {
            if !written_keys.contains(key) && !val.is_empty() {
                output_lines.push(format!("{key}={val}"));
            }
        }

        let result = output_lines.join("\n") + "\n";
        std::fs::write(&self.env_path, result).map_err(|e| format!("保存失败: {e}"))?;
        self.dirty = false;
        Ok(())
    }
}

/// Parse a .env file into key-value pairs.
fn parse_env_file(path: &Path) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Ok(content) = std::fs::read_to_string(path) {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some(eq_pos) = trimmed.find('=') {
                let key = trimmed[..eq_pos].trim().to_string();
                let val = trimmed[eq_pos + 1..].trim().to_string();
                map.insert(key, val);
            }
        }
    }
    map
}
