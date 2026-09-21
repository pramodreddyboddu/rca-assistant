"""Tests for the hash-chained audit log, including tamper detection."""

from audit import AuditLog


def test_append_and_verify(tmp_path):
    log = AuditLog(tmp_path / "a.jsonl")
    log.append("tool_call", "tok:…0001", {"tool": "get_queue_depth"})
    log.append("diagnosis_complete", "agent", {"top_hypothesis": "h1"})
    ok, msg = log.verify()
    assert ok, msg
    assert "3 entries" in msg  # genesis + 2


def test_genesis_prev_marker(tmp_path):
    log = AuditLog(tmp_path / "a.jsonl")
    first = log.entries()[0]
    assert first["seq"] == 0
    assert first["prev"] == "GENESIS"


def test_chain_links(tmp_path):
    log = AuditLog(tmp_path / "a.jsonl")
    for i in range(5):
        log.append("e", "a", {"i": i})
    entries = log.entries()
    for prev_e, e in zip(entries, entries[1:]):
        assert e["prev"] == prev_e["hash"]
        assert e["seq"] == prev_e["seq"] + 1


def test_tamper_modified_entry_detected(tmp_path):
    p = tmp_path / "a.jsonl"
    log = AuditLog(p)
    log.append("tool_call", "tok:…0001", {"tool": "get_queue_depth"})
    log.append("tool_call", "tok:…0001", {"tool": "restart_channel"})
    lines = p.read_text().splitlines()
    # Modify the details of entry seq 1 without fixing the hash.
    import json
    entry = json.loads(lines[1])
    entry["details"] = {"tool": "forged"}
    lines[1] = json.dumps(entry)
    p.write_text("\n".join(lines) + "\n")
    ok, msg = AuditLog(p).verify()
    assert not ok
    assert "seq 1" in msg


def test_tamper_deleted_entry_detected(tmp_path):
    p = tmp_path / "a.jsonl"
    log = AuditLog(p)
    log.append("e1", "a", {})
    log.append("e2", "a", {})
    lines = p.read_text().splitlines()
    del lines[1]  # remove seq 1
    p.write_text("\n".join(lines) + "\n")
    ok, msg = AuditLog(p).verify()
    assert not ok


def test_corrupt_line_detected_not_raised(tmp_path):
    p = tmp_path / "a.jsonl"
    log = AuditLog(p)
    log.append("e1", "a", {})
    with p.open("a") as fh:
        fh.write("this is not json\n")
    ok, msg = AuditLog(p).verify()
    assert not ok
    assert "corrupt" in msg.lower()


def test_summary(tmp_path):
    log = AuditLog(tmp_path / "a.jsonl")
    log.append("tool_call", "a", {})
    log.append("tool_call", "a", {})
    s = log.summary()
    assert s["entries"] == 3
    assert s["events"]["tool_call"] == 2
