# Custom Instructions for pandera-unified-validator Development

- Use Python 3.10+ syntax (match-case, | for unions, new type hint syntax)
- Prefer composition over inheritance
- Use dataclasses or Pydantic models for data structures
- Write docstrings in Google style format
- Type hints required on all public APIs
- Use descriptive variable names (no single-letter except i, j for loops)
- Prefer explicit over implicit
- Use pathlib.Path over string paths
- Follow PEP 8 with line length 100
- Test file naming: test_<module>.py
- Use pytest fixtures over setUp/tearDown
- Async functions should have async_ prefix
