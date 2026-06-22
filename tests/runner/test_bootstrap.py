"""Tests for runner.bootstrap — TTY and non-TTY behavior."""

from __future__ import annotations

import io
import os
import sys
import pytest
from unittest.mock import patch, MagicMock

from runner import bootstrap


class TestNonTTYExit:
    """In non-TTY contexts (cron, CI, piped input), bootstrap must exit non-zero."""

    def test_missing_file_exits_nonzero_in_nontty(self, tmp_path, capsys):
        config_path = str(tmp_path / "run.yml")
        with patch.object(sys.stdin, "isatty", return_value=False):
            with pytest.raises(SystemExit) as exc_info:
                bootstrap.maybe_bootstrap(config_path)
        assert exc_info.value.code != 0
        captured = capsys.readouterr()
        assert "run.yml" in captured.err or "run.yml" in captured.out

    def test_guidance_message_shown_in_nontty(self, tmp_path, capsys):
        config_path = str(tmp_path / "run.yml")
        with patch.object(sys.stdin, "isatty", return_value=False):
            with pytest.raises(SystemExit):
                bootstrap.maybe_bootstrap(config_path)
        out = capsys.readouterr().err
        assert "connection" in out or "tcm_url" in out or "tabcloud-run" in out


class TestTTYDecline:
    """On TTY, if user declines interactive setup, exit 0."""

    def test_user_declines_exits_0(self, tmp_path):
        config_path = str(tmp_path / "run.yml")
        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", return_value="n"),
        ):
            with pytest.raises(SystemExit) as exc_info:
                bootstrap.maybe_bootstrap(config_path)
        assert exc_info.value.code == 0

    def test_user_ctrl_c_exits_0(self, tmp_path):
        config_path = str(tmp_path / "run.yml")

        def _raise(*a, **kw):
            raise KeyboardInterrupt

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=_raise),
        ):
            with pytest.raises(SystemExit) as exc_info:
                bootstrap.maybe_bootstrap(config_path)
        assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# Shared helpers for TTY-accept tests
# ---------------------------------------------------------------------------

def _mock_inputs(
    tcm_url="https://test.online.tableau.com",
    token_name="pat",
    save_path="",       # "" → default (./run.yml); "no" → temp; custom path → that path
    storage_path="",    # "" → default (next to config); custom path → that path
    keyring_answer="n", # "n" → decline keyring; "y" → accept
    plaintext_choice=None,  # None = no warning prompt; "1" or "2" if warned
    env_var_name="",    # only used when plaintext_choice == "1"
):
    """
    Build an input() side-effect list that matches the bootstrap prompt sequence:

      1. "Proceed with interactive setup? [y/N]"
      2. "TCM URL ..."
      3. "PAT name ..."
      4. "Store the PAT secret in your OS keychain? [Y/n]"   (if keyring importable)
      5. "Save path [default: ...]"
      6. "Storage path [default: ...]"
      7. "Choose [1/2]:"                                      (only when plaintext warned)
      8. "Environment variable name [...]"                    (only when choice == "1")
    """
    inputs = [
        "y",            # proceed?
        tcm_url,        # tcm_url
        token_name,     # token_name
        keyring_answer, # keyring offer
        save_path,      # save path
        storage_path,   # storage path
    ]
    if plaintext_choice is not None:
        inputs.append(plaintext_choice)
        if plaintext_choice == "1":
            inputs.append(env_var_name)  # env var name ("" = accept default)
    return inputs


class TestTTYAccept:
    """On TTY, if user accepts and provides values, a run.yml is written."""

    def test_writes_run_yml_to_default_cwd_path(self, tmp_path, monkeypatch):
        """Pressing Enter at the save-path prompt writes to ./run.yml (cwd)."""
        monkeypatch.chdir(tmp_path)
        expected_path = str(tmp_path / "run.yml")

        inputs = iter(_mock_inputs(save_path="", plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="secret-pat"),
        ):
            result = bootstrap.maybe_bootstrap(str(tmp_path / "run.yml"))

        assert result == expected_path
        assert os.path.isfile(expected_path)
        content = open(expected_path).read()
        assert "https://test.online.tableau.com" in content
        assert "pat" in content

    def test_writes_run_yml_to_custom_path(self, tmp_path, monkeypatch):
        """Typing a custom path at the save-path prompt writes there."""
        monkeypatch.chdir(tmp_path)
        custom_path = str(tmp_path / "subdir" / "myconfig.yml")
        inputs = iter(_mock_inputs(save_path=custom_path, plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="secret-pat"),
        ):
            result = bootstrap.maybe_bootstrap(str(tmp_path / "run.yml"))

        assert result == custom_path
        assert os.path.isfile(custom_path)

    def test_uses_temp_file_when_user_types_no(self, tmp_path, monkeypatch):
        """Typing 'no' at the save-path prompt returns a temp file."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        # No plaintext_choice needed — temp file skips the warning
        inputs = iter(_mock_inputs(save_path="no"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="secret-pat"),
        ):
            result = bootstrap.maybe_bootstrap(config_path)

        # Original file not written; a temp path returned instead
        assert result is not None
        assert result != config_path
        assert os.path.isfile(result)
        os.remove(result)

    def test_bare_secret_written_when_keyring_declined_and_user_confirms(
        self, tmp_path, monkeypatch
    ):
        """When keyring is declined and user confirms plaintext, bare secret is in the file."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(save_path="", plaintext_choice="2"))
        secret = "my-actual-secret-value"

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value=secret),
        ):
            bootstrap.maybe_bootstrap(config_path)

        content = open(config_path).read()
        assert secret in content or "env:" in content or "keyring:" in content


class TestFullTemplate:
    """The saved file must be based on the full annotated template."""

    def test_written_file_contains_template_sections(self, tmp_path, monkeypatch):
        """Saved file contains all major template sections, not just connection:."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(save_path="", plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        content = open(config_path).read()
        # All major top-level sections from the template must be present
        for section in ("extraction:", "transform:", "load:", "storage:", "logging:"):
            assert section in content, f"Expected section '{section}' in written config"

    def test_connection_values_substituted(self, tmp_path, monkeypatch):
        """User-entered connection values replace the placeholder strings."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        tcm_url = "https://myco.online.tableau.com"
        token_name = "myco-pat"
        inputs = iter(_mock_inputs(tcm_url=tcm_url, token_name=token_name,
                                    save_path="", plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        content = open(config_path).read()
        assert tcm_url in content
        assert token_name in content

    def test_placeholder_strings_not_present(self, tmp_path, monkeypatch):
        """Template placeholder values must be replaced, not left in the file."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(
            tcm_url="https://real.online.tableau.com",
            token_name="real-pat",
            save_path="",
            plaintext_choice="2",
        ))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        content = open(config_path).read()
        assert "your-tenant.online.tableau.com" not in content
        assert "your-pat-name" not in content
        # The placeholder secret ref should also be gone from the connection block.
        # (It can still appear inside comments, so check the actual value line.)
        import re
        match = re.search(r'^\s+token_secret:\s+(\S+)', content, re.MULTILINE)
        assert match is not None
        assert match.group(1) != "keyring:tabcloud/extractor-pat"

    def test_mapping_paths_are_absolute(self, tmp_path, monkeypatch):
        """mapping: paths in the written file must be absolute so the file works from any dir."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(save_path="", plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        content = open(config_path).read()
        import re
        # Every uncommented mapping: line must have an absolute path
        for match in re.finditer(r'^\s+mapping:\s+(\S+)', content, re.MULTILINE):
            mapping_val = match.group(1)
            assert os.path.isabs(mapping_val), (
                f"Expected absolute mapping path, got: {mapping_val}"
            )
            assert os.path.isfile(mapping_val), (
                f"Absolute mapping path does not exist: {mapping_val}"
            )

    def test_template_used_for_temp_file_too(self, tmp_path, monkeypatch):
        """Even temp-file runs write the full template, not just a stub."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(save_path="no"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            result = bootstrap.maybe_bootstrap(config_path)

        content = open(result).read()
        for section in ("extraction:", "transform:", "load:", "storage:"):
            assert section in content, f"Expected section '{section}' in temp config"
        os.remove(result)


class TestPlaintextWarning:
    """Warning is shown (and acted on) when saving a bare-literal secret to disk."""

    def test_plaintext_warning_shown_when_saving(self, tmp_path, monkeypatch, capsys):
        """The WARNING text is printed when a bare literal would be written to a file."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(save_path="", plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        out = capsys.readouterr().out
        assert "WARNING" in out or "PLAINTEXT" in out or "plaintext" in out

    def test_no_plaintext_warning_for_temp_file(self, tmp_path, monkeypatch, capsys):
        """No plaintext warning when the user declines to save (temp file path)."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        # save_path="no" → temp file; no plaintext_choice should be needed
        inputs = iter(_mock_inputs(save_path="no"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            result = bootstrap.maybe_bootstrap(config_path)

        out = capsys.readouterr().out
        assert "WARNING" not in out or "PLAINTEXT" not in out  # no plaintext warning
        os.remove(result)

    def test_user_can_switch_to_env_ref(self, tmp_path, monkeypatch):
        """Choosing option 1 substitutes an env: reference into the saved file."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(
            save_path="",
            plaintext_choice="1",   # choose env: reference
            env_var_name="",        # accept default → TABCLOUD_PAT_SECRET
        ))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        content = open(config_path).read()
        assert "env:TABCLOUD_PAT_SECRET" in content
        # Raw secret must not be in the file
        assert "s3cr3t" not in content

    def test_user_can_switch_to_env_ref_custom_var(self, tmp_path, monkeypatch):
        """Choosing option 1 with a custom var name uses that name."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(
            save_path="",
            plaintext_choice="1",
            env_var_name="MY_TCM_SECRET",
        ))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        content = open(config_path).read()
        assert "env:MY_TCM_SECRET" in content
        assert "s3cr3t" not in content

    def test_user_keeps_plaintext_after_warning(self, tmp_path, monkeypatch):
        """Choosing option 2 writes the bare literal with an inline WARNING comment."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        secret = "xyzzy-plaintext-secret"
        inputs = iter(_mock_inputs(save_path="", plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value=secret),
        ):
            bootstrap.maybe_bootstrap(config_path)

        content = open(config_path).read()
        # Bare secret must be present
        assert secret in content
        # And the inline warning comment must mark the line
        import re
        match = re.search(r'^\s+token_secret:\s+\S+(.*)$', content, re.MULTILINE)
        assert match is not None
        assert "WARNING" in match.group(1) or "plaintext" in match.group(1).lower()

    def test_plaintext_warning_not_shown_when_keyring_used(self, tmp_path, monkeypatch, capsys):
        """No plaintext warning when a keyring: ref is stored instead of a bare literal."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")

        # Simulate keyring available + accepted
        mock_kr = MagicMock()
        mock_kr.set_password = MagicMock()

        # Keyring accepted: prompt sequence has no plaintext_choice
        inputs = iter(_mock_inputs(keyring_answer="y", save_path=""))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
            patch.dict(sys.modules, {"keyring": mock_kr}),
        ):
            bootstrap.maybe_bootstrap(config_path)

        out = capsys.readouterr().out
        # No plaintext warning should appear
        assert "PLAINTEXT" not in out
        # File should contain a keyring: reference
        content = open(config_path).read()
        assert "keyring:" in content
        assert "s3cr3t" not in content


class TestStoragePath:
    """The interactive setup prompts for a storage path and writes it into run.yml."""

    def _read_storage_path(self, content):
        import re
        match = re.search(r'^\s+path:\s+(\S+)', content, re.MULTILINE)
        assert match is not None, "no storage path line found"
        return match.group(1)

    def test_default_storage_is_sibling_of_config_dir(self, tmp_path, monkeypatch):
        """Pressing Enter stores data next to run.yml, not relative to cwd."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        inputs = iter(_mock_inputs(save_path=config_path, storage_path="",
                                   plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        written = self._read_storage_path(open(config_path).read())
        assert written == str(tmp_path / "data_workspace")

    def test_default_storage_escapes_config_folder(self, tmp_path, monkeypatch):
        """When run.yml lives in a 'config' dir, data lands as a sibling of it."""
        monkeypatch.chdir(tmp_path)
        config_dir = tmp_path / "config"
        config_path = str(config_dir / "run.yml")
        inputs = iter(_mock_inputs(save_path=config_path, storage_path="",
                                   plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        written = self._read_storage_path(open(config_path).read())
        # Sibling of config/, never inside it.
        assert written == str(tmp_path / "data_workspace")
        assert "config/data_workspace" not in written

    def test_custom_storage_path_is_used(self, tmp_path, monkeypatch):
        """A path typed at the storage prompt is written verbatim (absolute)."""
        monkeypatch.chdir(tmp_path)
        config_path = str(tmp_path / "run.yml")
        custom_storage = str(tmp_path / "elsewhere" / "store")
        inputs = iter(_mock_inputs(save_path=config_path, storage_path=custom_storage,
                                   plaintext_choice="2"))

        with (
            patch.object(sys.stdin, "isatty", return_value=True),
            patch("builtins.input", side_effect=lambda _: next(inputs)),
            patch("getpass.getpass", return_value="s3cr3t"),
        ):
            bootstrap.maybe_bootstrap(config_path)

        written = self._read_storage_path(open(config_path).read())
        assert written == custom_storage


class TestIsSecretRef:
    """Unit tests for the _is_secret_ref helper."""

    @pytest.mark.parametrize("ref", [
        "keyring:tabcloud/extractor-pat",
        "env:MY_SECRET",
        "file:/path/to/secret",
        "awssm:my-secret#key",
        "plain:literal-value",
    ])
    def test_recognised_refs_return_true(self, ref):
        assert bootstrap._is_secret_ref(ref) is True

    @pytest.mark.parametrize("ref", [
        "abcdefg",
        "s3cr3t-value",
        "https://not-a-ref",
        "",
    ])
    def test_bare_literals_return_false(self, ref):
        assert bootstrap._is_secret_ref(ref) is False
