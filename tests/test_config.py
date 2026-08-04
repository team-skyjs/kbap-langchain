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
