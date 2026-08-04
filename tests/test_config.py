import pytest
from pydantic import ValidationError

import textwrap

from kbap_review.config import load_config


def test_load_config(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
          avoidance_model: gpt-5-mini
        thresholds:
          description: 70
          translations: 75
          avoidance: 80
        concurrency: 3
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "secret-token")

    cfg = load_config(str(cfg_file))

    assert cfg.kbap_base_url == "http://kbap.example.com"
    assert cfg.kbap_token == "secret-token"
    assert cfg.model == "gemini-2.5-flash"
    assert cfg.avoidance_model == "gpt-5-mini"
    assert cfg.thresholds.description == 70
    assert cfg.thresholds.translations == 75
    assert cfg.thresholds.avoidance == 80
    assert cfg.concurrency == 3


def test_avoidance_model_defaults_to_model(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
        thresholds:
          description: 70
          translations: 70
          avoidance: 70
        concurrency: 5
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "t")

    cfg = load_config(str(cfg_file))

    assert cfg.avoidance_model == "gemini-2.5-flash"


def test_timeout_seconds_defaults_to_120(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
        thresholds:
          description: 70
          translations: 70
          avoidance: 70
        concurrency: 5
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "t")

    cfg = load_config(str(cfg_file))

    assert cfg.timeout_seconds == 120


def test_timeout_seconds_is_configurable(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
          timeout_seconds: 45
        thresholds:
          description: 70
          translations: 70
          avoidance: 70
        concurrency: 5
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "t")

    cfg = load_config(str(cfg_file))

    assert cfg.timeout_seconds == 45


def _write(tmp_path, thresholds: str, concurrency: int):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "kbap_api:\n"
        "  base_url: http://kbap.example.com\n"
        "llm:\n"
        "  model: openai:gpt-5-mini\n"
        f"thresholds:\n{thresholds}"
        f"concurrency: {concurrency}\n"
    )
    return str(cfg)


OK_THRESHOLDS = "  description: 70\n  translations: 70\n  avoidance: 70\n"


def test_rejects_nonpositive_concurrency(tmp_path, monkeypatch):
    # Semaphore(0) 은 모든 코루틴을 영구히 막아 무인 배치가 조용히 멈춘다.
    monkeypatch.setenv("KBAP_API_TOKEN", "t")
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, OK_THRESHOLDS, 0))


def test_rejects_threshold_outside_score_range(tmp_path, monkeypatch):
    # 음수면 전부 통과, 100 초과면 전부 탈락한다.
    monkeypatch.setenv("KBAP_API_TOKEN", "t")
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, "  description: -1\n  translations: 70\n  avoidance: 70\n", 5))
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, "  description: 70\n  translations: 70\n  avoidance: 101\n", 5))
