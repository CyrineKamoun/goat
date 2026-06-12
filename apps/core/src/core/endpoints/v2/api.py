from fastapi import APIRouter

from . import (
    asset,
    custom_domain_lookup,
    datasets,
    folder,
    layer,
    organization_analytics,
    organization_domain,
    project,
    report_layout,
    status,
    system,
    user,
    workflow,
)

router = APIRouter()

router.include_router(user.router, prefix="/user", tags=["User"])
router.include_router(folder.router, prefix="/folder", tags=["Folder"])
router.include_router(layer.router, prefix="/layer", tags=["Layer"])
router.include_router(project.router, prefix="/project", tags=["Project"])
router.include_router(report_layout.router, prefix="/project", tags=["Report Layout"])
router.include_router(workflow.router, prefix="/project", tags=["Workflow"])
router.include_router(system.router, prefix="/system", tags=["System Settings"])
router.include_router(status.router, prefix="/status", tags=["Status"])
router.include_router(asset.router, prefix="/asset", tags=["Asset"])
router.include_router(datasets.router, prefix="/datasets", tags=["Datasets"])
router.include_router(
    organization_domain.router,
    prefix="/organizations/{organization_id}/domains",
    tags=["Organization Domain"],
)
router.include_router(
    organization_analytics.router,
    prefix="/organizations/{organization_id}/analytics",
    tags=["Organization Analytics"],
)
router.include_router(
    custom_domain_lookup.router,
    tags=["Custom Domain Lookup"],
)
