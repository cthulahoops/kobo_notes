import pytest
import sqlite3
import extract_annotations


@pytest.fixture
def status_db():
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()

    cursor.execute(extract_annotations.STATUS_DB_SCHEMA)
    connection.commit()

    yield connection

    connection.close()


def test_already_imported(status_db):
    test_annotation = {"BookmarkID": 1}
    extract_annotations.save_as_imported(status_db, "2020-01-01", [test_annotation])

    assert not extract_annotations.is_already_imported(
        status_db, test_annotation
    ), "The annotation is already imported but was not detected"


def test_not_imported(status_db):
    assert extract_annotations.is_already_imported(
        status_db, {"BookmarkID": 2}
    ), "The annotation is not imported but was detected as such"


def test_different_item_unimported(status_db):
    test_annotation = {"BookmarkID": 1}
    extract_annotations.save_as_imported(status_db, "2020-01-01", [test_annotation])

    assert extract_annotations.is_already_imported(
        status_db, {"BookmarkID": 2}
    ), "The annotation is not imported but was detected as such"
