from fastapi import APIRouter

from . import (
    asset,
    billing,
    custom_domain_lookup,
    catalog_records,
    datasets,
    folder,
    layer,
    organization_analytics,
    organization_domain,
    organizations,
    project,
    project_group,
    project_layer,
    project_public,
    report_layout,
    share,
    system,
    teams,
    users,
    webhooks,
    workflow,
)

router = APIRouter()

router.include_router(
    organizations.router, prefix="/organizations", tags=["Organizations"]
)
router.include_router(teams.router, prefix="/teams", tags=["Teams"])
router.include_router(users.router, prefix="/users", tags=["Users"])
router.include_router(share.router, prefix="/share", tags=["Share"])
router.include_router(billing.router, prefix="/billing", tags=["Billing"])
router.include_router(webhooks.router, prefix="/webhooks", tags=["Webhooks"])

router.include_router(folder.router, prefix="/folder", tags=["Folder"])
router.include_router(layer.router, prefix="/layer", tags=["Layer"])
router.include_router(catalog_records.router, prefix="/catalog/records", tags=["Catalog"])
router.include_router(project.router, prefix="/project", tags=["Project"])
router.include_router(
    project_layer.router, prefix="/project", tags=["Project Layers"]
)
router.include_router(
    project_public.router, prefix="/project", tags=["Project Public"]
)
router.include_router(
    project_group.router, prefix="/project", tags=["Project Layer Groups"]
)
router.include_router(report_layout.router, prefix="/project", tags=["Report Layout"])
router.include_router(workflow.router, prefix="/project", tags=["Workflow"])
router.include_router(system.router, prefix="/system", tags=["System Settings"])
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
