import os

import yaml
from dotenv import load_dotenv
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
    timeout_seconds: int = 120


def load_config(path: str = "config.yaml") -> AppConfig:
    # .env 를 상위 디렉터리까지 훑어 읽는다(노트북은 notebooks/ 에서 실행돼도 루트 .env 를 찾는다).
    # override=False 라 이미 설정된 환경변수가 우선 — cron/CI 가 준 값을 .env 가 덮지 않는다.
    load_dotenv()
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
        timeout_seconds=llm.get("timeout_seconds", 120),
    )
