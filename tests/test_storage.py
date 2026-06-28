import hashlib
import pytest
import pytest_asyncio

from agent.storage import LocalStorage, StorageError, FileNotFoundError, compute_sha256


pytestmark = pytest.mark.asyncio


async def _make_stream(data: bytes):
    """Helper — turn a bytes literal into an async iterator."""
    yield data


async def _write_test_file(storage: LocalStorage, content: bytes) -> str:
    file_hash = hashlib.sha256(content).hexdigest()
    total = await storage.write_stream(file_hash, _make_stream(content))
    assert total == len(content)
    return file_hash


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(tmp_path / "storage")


async def test_write_and_exists(storage):
    file_hash = await _write_test_file(storage, b"hello world")
    assert await storage.exists(file_hash) is True


async def test_exists_false_for_unknown(storage):
    assert await storage.exists("nonexistent_hash") is False


async def test_get_size(storage):
    content = b"some content"
    file_hash = await _write_test_file(storage, content)
    assert await storage.get_size(file_hash) == len(content)


async def test_get_size_raises_for_missing(storage):
    with pytest.raises(FileNotFoundError):
        await storage.get_size("missing")


async def test_read_full_range(storage):
    content = b"abcdefghij"
    file_hash = await _write_test_file(storage, content)
    stream = await storage.read_range(file_hash, 0, len(content) - 1)
    chunks = [chunk async for chunk in stream]
    assert b"".join(chunks) == content


async def test_read_partial_range(storage):
    content = b"abcdefghij"
    file_hash = await _write_test_file(storage, content)
    stream = await storage.read_range(file_hash, 2, 5)
    chunks = [chunk async for chunk in stream]
    assert b"".join(chunks) == b"cdef"


async def test_read_range_raises_for_missing(storage):
    with pytest.raises(FileNotFoundError):
        stream = await storage.read_range("missing", 0, 10)
        async for _ in stream:
            pass


async def test_delete(storage):
    file_hash = await _write_test_file(storage, b"to be deleted")
    await storage.delete(file_hash)
    assert await storage.exists(file_hash) is False


async def test_delete_nonexistent_is_noop(storage):
    await storage.delete("never_existed")  # should not raise


async def test_list_files(storage):
    h1 = await _write_test_file(storage, b"file one")
    h2 = await _write_test_file(storage, b"file two")
    files = await storage.list_files()
    assert set(files) == {h1, h2}


async def test_compute_sha256_utility():
    content = b"hash this content"
    expected = hashlib.sha256(content).hexdigest()
    result = await compute_sha256(_make_stream(content))
    assert result == expected


async def test_write_stream_cleans_up_on_failure(storage, tmp_path):
    file_hash = "will_fail"

    async def bad_stream():
        yield b"partial"
        raise RuntimeError("stream died")

    with pytest.raises(StorageError):
        await storage.write_stream(file_hash, bad_stream())

    assert await storage.exists(file_hash) is False