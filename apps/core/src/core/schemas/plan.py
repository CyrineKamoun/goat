from typing import List

from pydantic import BaseModel, Field

from core.utils.i18n import trans as _


class Plan(BaseModel):
    plan_name: str
    title: str
    description: str
    highlights: List[str] = Field(
        ..., description="List of key highlights and features of the plan"
    )


class PlansList(BaseModel):
    plans: List[Plan]


def get_plans_data() -> list:
    plans_data = [
        {
            "plan_name": _("goat_starter"),
            "title": _("Starter"),
            "description": _(
                "Ideal for individuals or small teams beginning with GOAT, offering essential features to explore its core capabilities."
            ),
            "highlights": [
                _("For small teams with up to 10 viewers and 3 editors"),
                _("Integrated and custom data"),
                _("Manage teams"),
                _("Share and collaborate"),
                _("Basic support"),
            ],
        },
        {
            "plan_name": _("goat_professional"),
            "title": _("Professional"),
            "description": _(
                "Designed for growing organizations, this plan offers advanced tools for deeper analysis and enhanced collaboration."
            ),
            "highlights": [
                _("For organization with up to 50 viewers and 5 editors"),
                _("Increased usage quota limits"),
                _("Premium support"),
            ],
        },
        {
            "plan_name": _("goat_enterprise"),
            "title": _("Enterprise"),
            "description": _(
                "Tailored for large organizations, this plan provides full-scale capabilities to manage multiple teams and projects at an enterprise level."
            ),
            "highlights": [
                _("For large enterprises with multiple departments"),
                _("Unlimited viewers and editors"),
                _("Custom branding and On-premise deployment"),
                _("Elite support"),
            ],
        },
    ]
    return plans_data
