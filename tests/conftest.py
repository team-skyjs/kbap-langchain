import pytest


@pytest.fixture(autouse=True)
def _no_langfuse(monkeypatch):
    # 프롬프트 테스트가 네트워크를 타지 않게 매 테스트 전에 키를 지운다.
    # load_config() 안의 load_dotenv()가 상위 .env 를 프로세스 env 에 로드해
    # 이후 테스트를 오염시키는 것도 여기서 차단된다 — 테스트는 항상 코드 폴백 경로만 검증한다.
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    # KBAP_API_BASE_URL 도 같은 이유로 지운다 — 개발자 .env 의 값이 load_config 테스트를 깨뜨리지 않게.
    monkeypatch.delenv("KBAP_API_BASE_URL", raising=False)
