"""Tests for metrics collection."""

from dagster_slurm.helpers.metrics import SlurmMetricsCollector


def test_parse_time():
    """Test time parsing."""
    collector = SlurmMetricsCollector()

    # Test various formats
    assert collector._parse_time("01:30") == 90.0  # MM:SS
    assert collector._parse_time("02:30:45") == 9045.0  # HH:MM:SS
    assert collector._parse_time("1-02:30:45") == 95445.0  # DD-HH:MM:SS
    assert collector._parse_time("00:00:00") == 0.0


def test_parse_memory():
    """Test memory parsing."""
    collector = SlurmMetricsCollector()

    assert collector._parse_memory("1024K") == 1.0  # 1MB
    assert collector._parse_memory("512M") == 512.0
    assert collector._parse_memory("2G") == 2048.0
    assert collector._parse_memory("1T") == 1048576.0


def test_parse_exit_code():
    """Test exit code parsing."""
    collector = SlurmMetricsCollector()

    assert collector._parse_exit_code("0:0") == 0
    assert collector._parse_exit_code("1:0") == 1
    assert collector._parse_exit_code("127:15") == 127
