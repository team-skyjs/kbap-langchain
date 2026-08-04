import os

import yaml
from pydantic import BaseModel


class Thresholds(BaseModel):
    description: int
    translations: int
    avoidance: int


class AppConfig(BaseModel):
    kbap_base_url: str
    kbap_token: str
    model: str
    avoidance_model: str
    thresholds: Thresholds
    concurrency: int


def load_config(path: str = "config.yaml") -> AppConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    llm = raw["llm"]
    return AppConfig(
        kbap_base_url=raw["kbap_api"]["base_url"],
        kbap_token=os.environ["KBAP_API_TOKEN"],
        model=llm["model"],
        avoidance_model=llm.get("avoidance_model", llm["model"]),
        thresholds=Thresholds(**raw["thresholds"]),
        concurrency=raw["concurrency"],
    )
