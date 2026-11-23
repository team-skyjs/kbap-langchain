"""SQS + Lambda 대체 (2026-11-23, KB-1028). 홈서버 ElasticMQ 큐를 폴링해 Lambda 핸들러를 그대로 호출한다.
실행: python -m kbap.poller (환경변수 SQS_ENDPOINT·QUEUE_NAME 필수, POLL_BATCH 선택)
ponytail: 실패한 메시지도 지운다 — 재시도는 서버 outbox의 24시간 복구(recoverStale)가 맡는다.
여기서 다시 넣으면 같은 음식에 LLM 비용이 끝없이 나간다. 처리량이 모자라면 BATCH를 키운다.
"""
import os
import time
import traceback

import boto3

from kbap.main import handler

ENDPOINT = os.environ["SQS_ENDPOINT"]
QUEUE = os.environ["QUEUE_NAME"]
BATCH = int(os.environ.get("POLL_BATCH", "5"))

sqs = boto3.client("sqs", endpoint_url=ENDPOINT, region_name="ap-northeast-2",
                   aws_access_key_id="x", aws_secret_access_key="x")
url = f"{ENDPOINT}/000000000000/{QUEUE}"


def main():
    while True:
        try:
            sqs.create_queue(QueueName=QUEUE, Attributes={"VisibilityTimeout": "900"})
            break
        except Exception as e:  # 큐 컨테이너가 아직 안 떴을 때
            print(f"poller: queue not ready ({e!r}), retry in 5s", flush=True)
            time.sleep(5)
    print(f"poller: listening {url} batch={BATCH}", flush=True)
    while True:
        try:
            msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=BATCH, WaitTimeSeconds=20).get("Messages", [])
        except Exception as e:
            print(f"poller: receive failed {e!r}", flush=True)
            time.sleep(5)
            continue
        if not msgs:
            continue
        event = {"Records": [{"messageId": m["MessageId"], "body": m["Body"]} for m in msgs]}
        t = time.time()
        try:
            result = handler(event, None)
        except Exception:
            traceback.print_exc()
            result = "handler raised"
        print(f"poller: {len(msgs)} msgs in {time.time() - t:.0f}s -> {result}", flush=True)
        for m in msgs:
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])


if __name__ == "__main__":
    main()
