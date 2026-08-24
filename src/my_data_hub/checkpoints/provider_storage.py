"""Provider-safe physical names for brokered checkpoint Dataset blobs.

Kaggle normalizes path components and expands files whose names look like
archives.  The canonical checkpoint contract must keep its logical paths and
archive bytes unchanged, so the provider-facing names are deliberately flat,
opaque, and non-archive-looking.  Notebook bootstraps materialize these blobs
back to the canonical package paths before restore or verification.
"""

from __future__ import annotations

from collections.abc import Iterable

CHECKPOINT_PROVIDER_FILE_NAMES: dict[str, str] = {
    "checkpoint-manifest.json": "mdh-v1-checkpoint-manifest-json.blob",
    "logical/hub.dump": "mdh-v1-logical-hub-dump.blob",
    "physical/backup_manifest": "mdh-v1-physical-backup-manifest.blob",
    "physical/base.tar.gz": "mdh-v1-physical-base-tar-gz.blob",
    "physical/pg_wal.tar.gz": "mdh-v1-physical-pg-wal-tar-gz.blob",
    "receipts/verification.json": "mdh-v1-receipts-verification-json.blob",
}

CHECKPOINT_LOGICAL_FILE_NAMES: dict[str, str] = {
    provider_name: logical_name
    for logical_name, provider_name in CHECKPOINT_PROVIDER_FILE_NAMES.items()
}


def checkpoint_provider_file_name(logical_name: str) -> str:
    """Return the fixed opaque provider name for one canonical package path."""

    try:
        return CHECKPOINT_PROVIDER_FILE_NAMES[logical_name]
    except KeyError as exc:
        raise ValueError("checkpoint logical file is outside the fixed provider contract") from exc


def checkpoint_provider_name_map(logical_names: Iterable[str]) -> dict[str, str]:
    """Return an exact provider-name to logical-name map for a fixed file set."""

    result: dict[str, str] = {}
    for logical_name in logical_names:
        provider_name = checkpoint_provider_file_name(logical_name)
        if provider_name in result:
            raise ValueError("checkpoint provider file name is duplicated")
        result[provider_name] = logical_name
    return result


def checkpoint_materializer_source() -> str:
    """Return a credential-free stdlib bootstrap for exact package materialization.

    The returned source is embedded in Kaggle Notebooks before the project
    wheel is installed.  It therefore imports only the Python standard
    library and treats the provider mount as immutable input.
    """

    provider_to_logical = repr(CHECKPOINT_LOGICAL_FILE_NAMES)
    manifest_provider_name = CHECKPOINT_PROVIDER_FILE_NAMES["checkpoint-manifest.json"]
    return f'''\
def _mdh_materialize_checkpoint(_input_root, _expected_manifest_sha256, _destination):
    import hashlib as _checkpoint_hashlib
    import json as _checkpoint_json
    import os as _checkpoint_os
    import pathlib as _checkpoint_pathlib
    _provider_to_logical = {provider_to_logical}
    _manifest_provider_name = {manifest_provider_name!r}
    _input_root = _checkpoint_pathlib.Path(_input_root)
    _destination = _checkpoint_pathlib.Path(_destination)
    if not _input_root.is_dir() or _input_root.is_symlink():
        raise RuntimeError('checkpoint provider input root is unavailable')
    _manifest_matches = []
    for _index, _candidate in enumerate(_input_root.rglob(_manifest_provider_name)):
        if _index >= 4096:
            raise RuntimeError('checkpoint provider input discovery exceeds bound')
        _relative = _candidate.relative_to(_input_root)
        if (_candidate.is_symlink() or not _candidate.is_file() or
                any(_input_root.joinpath(*_relative.parts[:_part]).is_symlink()
                    for _part in range(1, len(_relative.parts))) or
                _candidate.stat().st_size > 1048576):
            continue
        try:
            _payload = _checkpoint_json.loads(_candidate.read_bytes())
        except (OSError, ValueError):
            continue
        if _payload.get('manifest_sha256') == _expected_manifest_sha256:
            _manifest_matches.append((_candidate, _payload))
    if len(_manifest_matches) != 1:
        raise RuntimeError('exact checkpoint provider input is absent or ambiguous')
    _manifest_source, _manifest_payload = _manifest_matches[0]
    _source_root = _manifest_source.parent
    _observed_names = []
    for _index, _source in enumerate(_source_root.rglob('*')):
        if _index >= 64:
            raise RuntimeError('checkpoint provider file inventory exceeds bound')
        _relative = _source.relative_to(_source_root)
        if (_source.is_symlink() or
                any(_source_root.joinpath(*_relative.parts[:_part]).is_symlink()
                    for _part in range(1, len(_relative.parts)))):
            raise RuntimeError('checkpoint provider input contains a symlink')
        if _source.is_file():
            if len(_relative.parts) != 1:
                raise RuntimeError('checkpoint provider file is not flat')
            _observed_names.append(_relative.as_posix())
    if set(_observed_names) != set(_provider_to_logical) or len(_observed_names) != len(_provider_to_logical):
        raise RuntimeError('checkpoint provider file set differs')
    _manifest_files = _manifest_payload.get('files')
    if not isinstance(_manifest_files, list):
        raise RuntimeError('checkpoint manifest file claims are absent')
    _claims = {{}}
    for _claim in _manifest_files:
        if (not isinstance(_claim, dict) or set(_claim) != {{'byte_size', 'kind', 'path', 'sha256'}} or
                not isinstance(_claim.get('path'), str) or
                not isinstance(_claim.get('byte_size'), int) or isinstance(_claim.get('byte_size'), bool) or
                not 1 <= _claim['byte_size'] <= 10737418240 or
                not isinstance(_claim.get('sha256'), str) or len(_claim['sha256']) != 64 or
                any(_character not in '0123456789abcdef' for _character in _claim['sha256']) or
                _claim['path'] in _claims):
            raise RuntimeError('checkpoint manifest file claim is invalid')
        _claims[_claim['path']] = (_claim['byte_size'], _claim['sha256'])
    _expected_logical = set(_provider_to_logical.values()) - {{'checkpoint-manifest.json'}}
    if set(_claims) != _expected_logical:
        raise RuntimeError('checkpoint manifest file set differs from provider contract')
    if (_destination.exists() or _destination.is_symlink() or
            not _destination.parent.is_dir() or _destination.parent.is_symlink()):
        raise RuntimeError('checkpoint materialization destination is unsafe')
    _destination.mkdir(mode=0o700)
    for _provider_name, _logical_name in _provider_to_logical.items():
        _source = _source_root / _provider_name
        _target = _destination.joinpath(*_checkpoint_pathlib.PurePosixPath(_logical_name).parts)
        _target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _expected = _claims.get(_logical_name)
        _maximum = 1048576 if _expected is None else _expected[0]
        _source_fd = _checkpoint_os.open(_source, _checkpoint_os.O_RDONLY | _checkpoint_os.O_NOFOLLOW)
        try:
            _target_fd = _checkpoint_os.open(
                _target,
                _checkpoint_os.O_WRONLY | _checkpoint_os.O_CREAT | _checkpoint_os.O_EXCL | _checkpoint_os.O_NOFOLLOW,
                0o600,
            )
            try:
                _digest = _checkpoint_hashlib.sha256()
                _written = 0
                while True:
                    _block = _checkpoint_os.read(_source_fd, 1048576)
                    if not _block:
                        break
                    _written += len(_block)
                    if _written > _maximum:
                        raise RuntimeError('checkpoint provider blob exceeds its exact bound')
                    _digest.update(_block)
                    _checkpoint_os.write(_target_fd, _block)
                _checkpoint_os.fsync(_target_fd)
            finally:
                _checkpoint_os.close(_target_fd)
        finally:
            _checkpoint_os.close(_source_fd)
        if _expected is not None and (_written != _expected[0] or _digest.hexdigest() != _expected[1]):
            raise RuntimeError('checkpoint provider blob differs from manifest')
    return _destination, _source_root
'''
