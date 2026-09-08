import inspect
from typing import Any

from pydantic import BaseModel


def optional(*fields: Any) -> Any:
    def dec(_cls: type[BaseModel]) -> type[BaseModel]:
        for field in fields:
            _cls.model_fields[field].default = None
            # A field declared with a factory must not keep it: pydantic refuses
            # a field carrying both a default and a default_factory.
            _cls.model_fields[field].default_factory = None
            _cls.model_fields[field].annotation = Any
        _cls.model_rebuild(force=True)
        return _cls

    if fields and inspect.isclass(fields[0]) and issubclass(fields[0], BaseModel):
        cls = fields[0]
        fields = tuple(cls.model_fields.keys())
        return dec(cls)
    return dec
