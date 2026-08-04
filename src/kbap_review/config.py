import os

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class Thresholds(BaseModel):
    # 모델 점수가 0~100 이므로 임계값도 그 범위여야 한다. 음수면 전부 통과해
    # 기피성분 미달까지 REVIEWED 로 나가고, 100 초과면 전부 탈락한다.
    description: int = Field(ge=0, le=100)
    translations: int = Field(ge=0, le=100)
    avoidance: int = Field(ge=0, le=100)


class AppConfig(BaseModel):
    kbap_base_url: str
    kbap_token: str
    model: str
    avoidance_model: str
    thresholds: Thresholds
    # 0 이면 Semaphore(0) 이 모든 코루틴을 영구히 막아 무인 배치가 조용히 멈춘다.
    concurrency: int = Field(ge=1)
    timeout_seconds: int = Field(default=120, ge=1)


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
