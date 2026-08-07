# build 스테이지: uv.lock -> requirements.txt (최종 이미지에 uv 를 남기지 않기 위한 분리)
FROM public.ecr.aws/lambda/python:3.12 AS build
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project -o /requirements.txt

FROM public.ecr.aws/lambda/python:3.12
COPY --from=build /requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# .env 는 복사하지 않는다 — API 키는 Lambda 환경변수로 주입된다
COPY config.yaml ${LAMBDA_TASK_ROOT}/
COPY src/kbap ${LAMBDA_TASK_ROOT}/kbap
CMD ["kbap.main.handler"]
