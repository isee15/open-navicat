from PyQt6.QtWidgets import QDialog, QVBoxLayout, QFormLayout, QLineEdit, QDialogButtonBox, QMessageBox, QCheckBox, QSpinBox, QLabel
from PyQt6.QtCore import Qt
from typing import Dict, Any


class AISettingsDialog(QDialog):
    """Dialog to configure AI integration settings: base_url, model_name, api_key.

    Extended to include:
      - include_schema_in_prompt: whether to automatically include current DB schema in prompts
      - max_schema_chars: maximum characters of schema to include (truncated beyond this)
    """

    def __init__(self, parent=None, settings: Dict[str, Any] | None = None):
        super().__init__(parent)
        self.setWindowTitle("AI Settings")
        self.resize(480, 260)

        self._settings = settings or {}

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.base_url_edit = QLineEdit(self._settings.get("base_url", ""))
        self.base_url_edit.setPlaceholderText("https://api.example.com/v1")
        form.addRow("Base URL:", self.base_url_edit)

        self.model_edit = QLineEdit(self._settings.get("model_name", ""))
        self.model_edit.setPlaceholderText("e.g. gpt-4, local-model")
        form.addRow("Model name:", self.model_edit)

        self.api_key_edit = QLineEdit(self._settings.get("api_key", ""))
        self.api_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key_edit.setPlaceholderText("Your API key (kept locally)")
        form.addRow("API Key:", self.api_key_edit)

        # Show API key checkbox to allow temporarily revealing the token in the dialog
        self.show_key_chk = QCheckBox("Show API Key")
        # default unchecked: keep token hidden
        self.show_key_chk.setChecked(False)
        self.show_key_chk.toggled.connect(self._on_show_key_toggled)
        form.addRow("", self.show_key_chk)

        # Masked preview label (helpful if the setting came from an env var or stored value)
        self.api_key_preview = QLabel(self._mask_api_key(self._settings.get("api_key", "")))
        self.api_key_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        form.addRow("Key preview:", self.api_key_preview)

        # New: include schema checkbox
        include_schema_default = bool(self._settings.get("include_schema_in_prompt", True))
        self.include_schema_chk = QCheckBox("Include current DB schema in AI prompts")
        self.include_schema_chk.setChecked(include_schema_default)
        form.addRow("Include schema:", self.include_schema_chk)

        # Note: no max schema chars control — the full DB schema will be included in prompts.

        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._on_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _on_accept(self) -> None:
        """Validate inputs before accepting."""
        base = self.base_url_edit.text().strip()
        model = self.model_edit.text().strip()
        # api key may be empty (user might not want to set it now), but at minimum base URL is helpful
        if not base:
            QMessageBox.warning(self, "Validation", "Base URL is required")
            return
        # No max schema chars validation required
        self.accept()

    def get_data(self) -> Dict[str, Any]:
        """Return current settings as a dict."""
        return {
            "base_url": self.base_url_edit.text().strip(),
            "model_name": self.model_edit.text().strip(),
            "api_key": self.api_key_edit.text().strip(),
            "include_schema_in_prompt": bool(self.include_schema_chk.isChecked()),
        }

    def _on_show_key_toggled(self, checked: bool) -> None:
        """Toggle the API key QLineEdit echo mode and update preview when hiding."""
        try:
            if checked:
                self.api_key_edit.setEchoMode(QLineEdit.EchoMode.Normal)
                # when revealing, also update preview to full key for copy/paste
                self.api_key_preview.setText(self.api_key_edit.text() or "")
            else:
                self.api_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
                self.api_key_preview.setText(self._mask_api_key(self.api_key_edit.text() or self._settings.get("api_key", "")))
        except Exception:
            pass

    def _mask_api_key(self, key: str) -> str:
        """Return a masked preview of the API key keeping first/last few chars if long enough."""
        try:
            if not key:
                return "<none>"
            k = str(key)
            if len(k) <= 8:
                return "*" * len(k)
            return f"{k[:4]}{'*' * (len(k) - 8)}{k[-4:]}"
        except Exception:
            return "<hidden>"
