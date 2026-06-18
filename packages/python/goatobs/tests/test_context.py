"""Unit tests for goatobs.context — contextvars + bind_user_context."""
import asyncio

from goatobs.context import bind_user_context, get_user_context, set_user_context


def test_no_context_by_default():
    """Outside a bind_user_context block, get_user_context returns empty dict."""
    assert get_user_context() == {}


def test_bind_sets_context_inside_block():
    """Inside the context manager, bound fields are readable."""
    with bind_user_context(user_id="u1", email="a@b.com", realm="p4b"):
        ctx = get_user_context()
        assert ctx == {"user_id": "u1", "email": "a@b.com", "realm": "p4b"}


def test_bind_clears_context_on_exit():
    """After the context manager exits, the previous (empty) context is restored."""
    with bind_user_context(user_id="u1", email="a@b.com", realm="p4b"):
        pass
    assert get_user_context() == {}


def test_nested_bind_overrides_then_restores():
    """Nested binds shadow the outer context, restored on inner exit."""
    with bind_user_context(user_id="u1", email="a@b.com", realm="p4b"):
        with bind_user_context(user_id="u2", email="c@d.com", realm="p4b"):
            assert get_user_context()["user_id"] == "u2"
        assert get_user_context()["user_id"] == "u1"


def test_bind_accepts_extra_kwargs():
    """Extra kwargs (e.g. roles) are passed through into the context."""
    with bind_user_context(user_id="u1", email="a@b.com", realm="p4b", roles=["admin"]):
        ctx = get_user_context()
        assert ctx["roles"] == ["admin"]


def test_set_user_context_persists_within_task():
    """set_user_context survives until the asyncio task ends — no reset
    when leaving any particular block. This is what makes uvicorn access
    logs see user fields: they fire after FastAPI middleware unwinds,
    still inside the same task."""

    async def request_task():
        set_user_context(user_id="u1", email="a@b.com", realm="p4b")
        # Simulate the rest of the request after the auth middleware
        # has returned but before uvicorn writes the access log.
        await asyncio.sleep(0)
        return get_user_context()

    ctx_at_task_end = asyncio.run(request_task())
    assert ctx_at_task_end == {"user_id": "u1", "email": "a@b.com", "realm": "p4b"}


def test_set_user_context_does_not_leak_across_tasks():
    """Each task is its own contextvars copy, so a value set in one task
    is invisible to a sibling task. Regression test for the "won't leak
    between concurrent requests" claim."""

    async def setter():
        set_user_context(user_id="u_setter", email="a@b.com", realm="p4b")
        return get_user_context()

    async def reader():
        return get_user_context()

    async def driver():
        # Each task gets a fresh copy of the context via copy_context().
        a = await asyncio.create_task(setter())
        b = await asyncio.create_task(reader())
        return a, b

    setter_ctx, reader_ctx = asyncio.run(driver())
    assert setter_ctx["user_id"] == "u_setter"
    assert reader_ctx == {}
