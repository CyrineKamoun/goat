from pydantic import BaseModel


class OnboardingFacts(BaseModel):
    """Existence checks Home derives its stage and checklist from, rather
    than a stored onboarding flag."""

    has_project: bool
    has_uploaded_layer: bool
    has_catalog_layer: bool
    has_team: bool
    has_workflow: bool
