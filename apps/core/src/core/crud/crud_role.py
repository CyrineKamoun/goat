from core.crud.base import CRUDBase
from core.db.models import Role


class CRUDRole(CRUDBase[Role, Role, Role]):
    pass


role = CRUDRole(Role)
