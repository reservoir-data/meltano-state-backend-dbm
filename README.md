# `meltano-dbm-state-backend`

⚠️ **EXPERIMENTAL: See https://github.com/meltano/meltano/pull/8367.** ⚠️

<!--
[![PyPI version](https://img.shields.io/pypi/v/meltano-dbm-state-backend.svg?logo=pypi&logoColor=FFE873&color=blue)](https://pypi.org/project/meltano-dbm-state-backend)
[![Python versions](https://img.shields.io/pypi/pyversions/meltano-dbm-state-backend.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/meltano-dbm-state-backend)
-->

This is a [Meltano][meltano] extension that provides a [`dbm`][dbm] [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### With [pipx]

#### From GitHub

```bash
pipx install meltano
pipx inject 'meltano-dbm-state-backend @ git+https://github.com/edgarrmondragon/meltano-dbm-state-backend.git'
```
#### From PyPI

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
