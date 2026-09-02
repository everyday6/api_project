import json
import logging

import pandas as pd
import pytest

from src.common.suspect import (
    IS_SUSPECT_COLUMN,
    flag_suspect_pandas,
    log_quality_gate,
    suspect_ratio,
)


def _flagged(values):
    df = pd.DataFrame({"x": range(len(values))})
    return flag_suspect_pandas(df, pd.Series(values))


def test_suspect_ratio_counts_flagged_rows():
    assert suspect_ratio(_flagged([True, False, False, False])) == 0.25


def test_suspect_ratio_empty_frame_is_zero():
    empty = pd.DataFrame({"x": []})
    empty[IS_SUSPECT_COLUMN] = pd.Series([], dtype=bool)
    assert suspect_ratio(empty) == 0.0


def test_suspect_ratio_missing_column_raises():
    with pytest.raises(KeyError):
        suspect_ratio(pd.DataFrame({"x": [1, 2]}))


def _only_gate_line(caplog):
    lines = [
        r.getMessage() for r in caplog.records if r.getMessage().startswith("data_quality_gate ")
    ]
    assert len(lines) == 1
    return json.loads(lines[0].split(" ", 1)[1])


def test_log_quality_gate_emits_structured_line_on_pass(caplog):
    logger = logging.getLogger("test.quality_gate.pass")
    with caplog.at_level(logging.INFO, logger="test.quality_gate.pass"):
        log_quality_gate(
            logger,
            domain="speed",
            metric="suspect_ratio",
            value=0.031234567,
            threshold=0.20,
            passed=True,
            context="batch_end=2026-09-02T00:00:00",
        )

    payload = _only_gate_line(caplog)
    assert payload == {
        "event": "data_quality_gate",
        "domain": "speed",
        "metric": "suspect_ratio",
        "value": 0.031235,  # 6자리로 반올림
        "threshold": 0.20,
        "decision": "pass",
        "context": "batch_end=2026-09-02T00:00:00",
    }


def test_log_quality_gate_marks_block_when_not_passed(caplog):
    logger = logging.getLogger("test.quality_gate.block")
    with caplog.at_level(logging.INFO, logger="test.quality_gate.block"):
        log_quality_gate(
            logger,
            domain="lion",
            metric="conflict_ratio",
            value=0.04,
            threshold=0.01,
            passed=False,
            unique_keys=218000,
        )

    payload = _only_gate_line(caplog)
    assert payload["decision"] == "block"
    assert payload["domain"] == "lion"
    assert payload["metric"] == "conflict_ratio"
    assert payload["unique_keys"] == 218000
