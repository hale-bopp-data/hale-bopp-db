"""Tests for hb compile --engine redis — Redis configuration generator."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.compile import DataDictionary, load_dictionary
from app.core.redis_compile import (
    RedisCompileResult,
    RedisPattern,
    compile_redis,
    compile_redis_and_write,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_PATTERNS = [
    {
        "use_case": "config_cache",
        "entity": "configuration",
        "structure": "HASH",
        "key": "tenant:{tenant_id}:config",
        "ttl": 300,
        "strategy": "write-through",
    },
    {
        "use_case": "session_store",
        "entity": None,
        "structure": "STRING",
        "key": "session:{token}",
        "ttl": 3600,
        "strategy": "session",
    },
    {
        "use_case": "notification_rt",
        "entity": "notification",
        "structure": "PUBSUB",
        "key": "notify:{tenant_id}",
        "ttl": None,
        "strategy": "publish",
    },
    {
        "use_case": "chat_recent",
        "entity": "agent_chat_message",
        "structure": "LIST",
        "key": "chat:{conversation_id}:recent",
        "ttl": 1800,
        "strategy": "write-through",
        "max_items": 50,
    },
    {
        "use_case": "rate_limiting",
        "entity": "agent_execution",
        "structure": "STRING",
        "key": "agent:{agent_id}:calls:{minute}",
        "ttl": 120,
        "strategy": "increment",
    },
    {
        "use_case": "feature_flags",
        "entity": "configuration",
        "structure": "HASH",
        "key": "features:{tenant_id}",
        "ttl": 60,
        "strategy": "write-through",
    },
]


# ---------------------------------------------------------------------------
# RedisPattern model
# ---------------------------------------------------------------------------

class TestRedisPattern:
    def test_parse_hash_pattern(self):
        p = RedisPattern.model_validate(SAMPLE_PATTERNS[0])
        assert p.use_case == "config_cache"
        assert p.structure == "HASH"
        assert p.ttl == 300
        assert p.strategy == "write-through"

    def test_parse_pubsub_no_ttl(self):
        p = RedisPattern.model_validate(SAMPLE_PATTERNS[2])
        assert p.structure == "PUBSUB"
        assert p.ttl is None

    def test_parse_list_with_max_items(self):
        p = RedisPattern.model_validate(SAMPLE_PATTERNS[3])
        assert p.structure == "LIST"
        assert p.max_items == 50

    def test_null_entity_allowed(self):
        p = RedisPattern.model_validate(SAMPLE_PATTERNS[1])
        assert p.entity is None


# ---------------------------------------------------------------------------
# compile_redis
# ---------------------------------------------------------------------------

class TestCompileRedis:
    def test_returns_all_patterns(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert len(result.patterns) == 6

    def test_cli_script_not_empty(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert result.cli_script
        assert "hale-bopp-db" in result.cli_script

    def test_cli_script_contains_all_use_cases(self):
        result = compile_redis(SAMPLE_PATTERNS)
        for p in SAMPLE_PATTERNS:
            assert p["use_case"] in result.cli_script

    def test_cli_script_hash_commands(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert "HSET" in result.cli_script
        assert "HGETALL" in result.cli_script

    def test_cli_script_string_increment(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert "INCR" in result.cli_script

    def test_cli_script_list_commands(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert "LPUSH" in result.cli_script
        assert "LTRIM" in result.cli_script
        assert "LRANGE" in result.cli_script

    def test_cli_script_pubsub_commands(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert "PUBLISH" in result.cli_script
        assert "SUBSCRIBE" in result.cli_script

    def test_cli_script_ttl_commands(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert "EXPIRE" in result.cli_script or "SETEX" in result.cli_script

    def test_app_config_structure(self):
        result = compile_redis(SAMPLE_PATTERNS)
        config = result.app_config
        assert config["version"] == "1"
        assert "patterns" in config
        assert len(config["patterns"]) == 6

    def test_app_config_has_all_use_cases(self):
        result = compile_redis(SAMPLE_PATTERNS)
        config = result.app_config["patterns"]
        for p in SAMPLE_PATTERNS:
            assert p["use_case"] in config

    def test_app_config_ttl(self):
        result = compile_redis(SAMPLE_PATTERNS)
        config_cache = result.app_config["patterns"]["config_cache"]
        assert config_cache["ttl_seconds"] == 300
        assert config_cache["strategy"] == "write-through"

    def test_app_config_no_ttl_for_pubsub(self):
        result = compile_redis(SAMPLE_PATTERNS)
        notif = result.app_config["patterns"]["notification_rt"]
        assert "ttl_seconds" not in notif

    def test_app_config_max_items(self):
        result = compile_redis(SAMPLE_PATTERNS)
        chat = result.app_config["patterns"]["chat_recent"]
        assert chat["max_items"] == 50

    def test_app_config_source_entity(self):
        result = compile_redis(SAMPLE_PATTERNS)
        config_cache = result.app_config["patterns"]["config_cache"]
        assert config_cache["source_entity"] == "configuration"

    def test_docs_not_empty(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert result.docs
        assert "# Redis Patterns" in result.docs

    def test_docs_contains_all_use_cases(self):
        result = compile_redis(SAMPLE_PATTERNS)
        for p in SAMPLE_PATTERNS:
            assert p["use_case"] in result.docs

    def test_docs_contains_ttl_human(self):
        result = compile_redis(SAMPLE_PATTERNS)
        assert "5min" in result.docs  # 300s
        assert "1h" in result.docs    # 3600s
        assert "2min" in result.docs  # 120s

    def test_empty_patterns(self):
        result = compile_redis([])
        assert len(result.patterns) == 0
        assert result.app_config["patterns"] == {}


# ---------------------------------------------------------------------------
# compile_redis_and_write
# ---------------------------------------------------------------------------

class TestCompileRedisAndWrite:
    def test_writes_three_files(self, tmp_path: Path):
        result = compile_redis_and_write(SAMPLE_PATTERNS, tmp_path)
        assert (tmp_path / "redis-setup.sh").exists()
        assert (tmp_path / "redis-config.json").exists()
        assert (tmp_path / "redis-patterns.md").exists()

    def test_json_config_valid(self, tmp_path: Path):
        compile_redis_and_write(SAMPLE_PATTERNS, tmp_path)
        raw = json.loads((tmp_path / "redis-config.json").read_text(encoding="utf-8"))
        assert raw["version"] == "1"
        assert len(raw["patterns"]) == 6

    def test_creates_output_dir(self, tmp_path: Path):
        out = tmp_path / "deep" / "redis"
        compile_redis_and_write(SAMPLE_PATTERNS, out)
        assert out.exists()
        assert (out / "redis-config.json").exists()


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------

class TestRedisCompileCLI:
    def test_compile_redis_engine(self, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        # Create dict with redis_patterns
        dd = {
            "type_map": {"auto": {"pg": "BIGSERIAL"}},
            "default_map": {},
            "entities": [],
            "redis_patterns": SAMPLE_PATTERNS,
        }
        dict_file = tmp_path / "dict.json"
        dict_file.write_text(json.dumps(dd), encoding="utf-8")

        out_dir = tmp_path / "output"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "compile",
            "-e", "redis",
            "-d", str(dict_file),
            "-o", str(out_dir),
        ])

        assert result.exit_code == 0
        assert "Redis compile complete" in result.output
        assert "6" in result.output  # 6 patterns
        assert (out_dir / "redis-config.json").exists()

    def test_compile_redis_json_output(self, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        dd = {
            "type_map": {"auto": {"pg": "BIGSERIAL"}},
            "default_map": {},
            "entities": [],
            "redis_patterns": SAMPLE_PATTERNS,
        }
        dict_file = tmp_path / "dict.json"
        dict_file.write_text(json.dumps(dd), encoding="utf-8")

        out_dir = tmp_path / "output"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "compile",
            "-e", "redis",
            "-d", str(dict_file),
            "-o", str(out_dir),
            "-j",
        ])

        assert result.exit_code == 0
        # Find the JSON object in output (skip non-JSON lines)
        lines = result.output.strip().split("\n")
        json_start = next(i for i, l in enumerate(lines) if l.strip().startswith("{"))
        output = json.loads("\n".join(lines[json_start:]))
        assert output["version"] == "1"
        assert len(output["patterns"]) == 6

    def test_compile_redis_no_patterns(self, tmp_path: Path):
        from click.testing import CliRunner
        from app.cli import cli

        dd = {
            "type_map": {"auto": {"pg": "BIGSERIAL"}},
            "default_map": {},
            "entities": [],
        }
        dict_file = tmp_path / "dict.json"
        dict_file.write_text(json.dumps(dd), encoding="utf-8")

        out_dir = tmp_path / "output"
        runner = CliRunner()
        result = runner.invoke(cli, [
            "compile",
            "-e", "redis",
            "-d", str(dict_file),
            "-o", str(out_dir),
        ])

        assert result.exit_code == 0
        assert "No redis_patterns" in result.output


# ---------------------------------------------------------------------------
# Real dictionary integration
# ---------------------------------------------------------------------------

REAL_DICT_PATH = Path("C:/EW/easyway/wiki/guides/db-data-dictionary.json")


@pytest.mark.skipif(not REAL_DICT_PATH.exists(), reason="Real dictionary not found")
class TestRealDictionary:
    def test_load_redis_patterns(self):
        dd = load_dictionary(REAL_DICT_PATH)
        assert len(dd.redis_patterns) == 6

    def test_compile_real_redis(self):
        dd = load_dictionary(REAL_DICT_PATH)
        result = compile_redis(dd.redis_patterns)
        assert len(result.patterns) == 6
        assert result.cli_script
        assert result.app_config["patterns"]

    def test_compile_real_to_disk(self, tmp_path: Path):
        dd = load_dictionary(REAL_DICT_PATH)
        result = compile_redis_and_write(dd.redis_patterns, tmp_path)
        assert (tmp_path / "redis-setup.sh").exists()
        assert (tmp_path / "redis-config.json").exists()
        assert (tmp_path / "redis-patterns.md").exists()
        # Verify JSON is valid
        raw = json.loads((tmp_path / "redis-config.json").read_text(encoding="utf-8"))
        assert len(raw["patterns"]) == 6
