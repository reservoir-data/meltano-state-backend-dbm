# `meltano-state-backend-dbm`

This is a [Meltano][meltano] extension that provides a [`dbm`][dbm] [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano 'meltano-state-backend-dbm @ git+https://github.com/reservoir-data/meltano-state-backend-dbm.git'
```

#### With [uv]

```bash
uv tool install --with 'meltano-state-backend-dbm @ git+https://github.com/reservoir-data/meltano-state-backend-dbm.git' meltano
```

### From PyPI

_This package is not yet available on PyPI._

## Configuration

### `meltano.yml`

```yaml
state_backend:
  uri: dbm://${MELTANO_SYS_DIR_ROOT}/state
```

### Environment Variables

* `MELTANO_STATE_BACKEND_URI`: The URI of the DBM state backend.

[meltano]: https://meltano.com
[dbm]: https://docs.python.org/3/library/dbm.html
[state-backend]: https://docs.meltano.com/concepts/state_backends
[pipx]: https://github.com/pypa/pipx
[uv]: https://docs.astral.sh/uv
