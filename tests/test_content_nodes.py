from kbap_content.nodes import VALID_CODES, valid_substances


def test_valid_codes_covers_all_81_candidates():
    assert len(VALID_CODES) == 81
    assert "PORK" in VALID_CODES
    assert "SALTED_SHRIMP" in VALID_CODES


def test_valid_substances_drops_out_of_candidate_codes():
    # KB-236: 모델이 후보 밖 코드(김치 등)를 흘려도 저장 전에 걸러낸다.
    items = [
        {"code": "PORK", "inclusionPercent": 95},
        {"code": "KIMCHI", "inclusionPercent": 90},
    ]
    assert valid_substances(items) == [{"code": "PORK", "inclusionPercent": 95}]


def test_valid_substances_keeps_first_on_duplicate():
    items = [
        {"code": "PORK", "inclusionPercent": 95},
        {"code": "PORK", "inclusionPercent": 10},
    ]
    assert valid_substances(items) == [{"code": "PORK", "inclusionPercent": 95}]
