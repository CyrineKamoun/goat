import pytest
from core.schemas.invitations import InvitationOrgCreate


@pytest.mark.unit
def test_invitation_email_is_normalized_to_lowercase() -> None:
    invitation = InvitationOrgCreate(
        user_email="John.Doe@Example.COM", role="organization-editor"
    )
    assert invitation.user_email == "john.doe@example.com"


@pytest.mark.unit
def test_invitation_email_is_stripped() -> None:
    invitation = InvitationOrgCreate(
        user_email="  user@example.com ", role="organization-editor"
    )
    assert invitation.user_email == "user@example.com"
