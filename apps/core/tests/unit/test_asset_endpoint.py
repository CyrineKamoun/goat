from core.db.models.asset import AssetType
from core.endpoints.v2.asset import ALLOWED_MIME_TYPES, DOCUMENTS_MAX_FILE_SIZE_BYTES


def test_document_mime_types_defined() -> None:
    assert AssetType.DOCUMENT in ALLOWED_MIME_TYPES
    allowed = ALLOWED_MIME_TYPES[AssetType.DOCUMENT]
    assert "application/pdf" in allowed
    assert (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        in allowed
    )


def test_documents_max_file_size() -> None:
    assert DOCUMENTS_MAX_FILE_SIZE_BYTES == 52428800
