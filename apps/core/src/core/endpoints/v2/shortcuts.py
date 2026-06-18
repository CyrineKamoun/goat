from typing import Any

from fastapi import HTTPException

from core.crud.base import CRUDBase


async def get_object_or_404(crud: CRUDBase, *args: Any, **kwargs: Any) -> Any:
    obj = await crud.get(*args, **kwargs)
    if obj is None:
        raise HTTPException(
            status_code=404, detail=f"Object of {crud.model.__name__} not found"
        )
    else:
        return obj


async def get_multi_or_404(crud: CRUDBase, *args: Any, **kwargs: Any) -> Any:
    obj = await crud.get_multi(*args, **kwargs)
    if obj is None:
        raise HTTPException(
            status_code=404, detail=f"No objects of {crud.model.__name__} found"
        )
    else:
        return obj


async def update_or_404(
    crud: CRUDBase, *args: Any, db_obj: Any = None, **kwargs: Any
) -> Any:
    get_kwargs = {
        "id": kwargs["obj_in"].id,
        "db": kwargs.get("db", None),
    }
    obj = await get_object_or_404(crud, *args, **get_kwargs)
    if db_obj is None:
        db_obj = obj
    return await crud.update(*args, **kwargs, db_obj=db_obj)


async def delete_or_404(crud: CRUDBase, *args: Any, **kwargs: Any) -> Any:
    await get_object_or_404(crud, *args, **kwargs)
    return await crud.delete(*args, **kwargs)
