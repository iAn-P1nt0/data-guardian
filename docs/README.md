# Architecture Overview

`pandera-unified-validator` is organized around three layers:

1. **Core** – `ValidationSchema` models combine Pydantic record checks with Pandera dataframe
   schemas. `DataGuardianValidator` orchestrates backend selection based on the incoming frame
   type and composes the resulting `ValidationReport` objects. The higher-level
   `UnifiedValidator` adds typed `ValidationResult` containers, streaming helpers, auto-fix
   suggestions, and pretty console rendering via `rich` for developer ergonomics.
2. **Backends** – `PandasBackend` and `PolarsBackend` implement the `ValidationBackend`
   protocol. Backends are responsible for (a) asserting they can handle the supplied data
   structure and (b) translating the core schema contracts into engine-specific execution.
3. **Utilities** – `ValidationReport` captures normalized validation outcomes including
   errors, warnings, and diagnostic metadata. Reports can be merged and annotated so callers
   receive a single, composable summary regardless of how many validation phases ran.

## Extending Backends

Implement a new backend by conforming to `ValidationBackend` and registering it on a
`DataGuardianValidator` instance:

```python
from pandera_unified_validator.core import ValidationBackend
from pandera_unified_validator.utils import ValidationReport


class DaskBackend(ValidationBackend["dask.dataframe.DataFrame"]):
    name = "dask"

    def supports(self, data: object) -> bool:
        import dask.dataframe as dd
        return isinstance(data, dd.DataFrame)

    def validate(self, data, schema):
        pdf = data.compute()
        return schema.validate_dataframe(pdf).with_metadata(backend=self.name)
```

## Documentation Roadmap

- [ ] Usage guide for Polars pipelines
- [ ] Recipes for incremental validation in ETL workflows
- [ ] CLI helper for running validations as part of CI/CD
