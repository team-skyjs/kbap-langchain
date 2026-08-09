"""prompts.PROMPTS 레지스트리 중 Langfuse 에 없는 프롬프트만 생성한다.

이미 있는 프롬프트는 건드리지 않는다 — 콘솔에서 튜닝된 production 버전을
코드 템플릿으로 덮어쓰면 튜닝이 유실된다. 갱신은 Langfuse 콘솔에서 한다.

  uv run python scripts/upload_prompts.py
"""

from dotenv import load_dotenv

load_dotenv()

from langfuse import get_client  # noqa: E402 — .env 로드 후에 클라이언트를 만든다

from kbap.prompts import PROMPTS  # noqa: E402

client = get_client()
for name, template in PROMPTS.items():
    try:
        client.get_prompt(name, label="production", fallback=None)
        print(f"유지: {name} (이미 존재)")
    except Exception:
        client.create_prompt(name=name, prompt=template, labels=["production"], type="text")
        print(f"생성: {name}")
