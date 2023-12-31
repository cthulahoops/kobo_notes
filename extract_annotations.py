from collections import defaultdict
from datetime import datetime
import itertools
from functools import partial
from pathlib import Path
import click
import sqlite3
import html2text
from jinja2 import Environment, FileSystemLoader

ANNOTATIONS = """
    select
    Bookmark.BookmarkID,
    Bookmark.ContentID,
    Bookmark.VolumeID,
    Bookmark.Text,
    Bookmark.Annotation,
    Bookmark.DateCreated,
    Bookmark.DateModified,
    volume.BookTitle,
    volume.Attribution,
    content.Title
    from Bookmark
    join content on Bookmark.ContentID = content.ContentID
    join content volume on Bookmark.VolumeID = volume.ContentID
    where Bookmark.Type = 'highlight'
    order by Bookmark.DateCreated;
"""

BOOKS = """
    select
        volume.ContentID,
        volume.BookTitle,
        volume.Title,
        volume.SubTitle,
        volume.Attribution,
        volume.Description
    from content volume
    where ContentType = '6' and volume.Title is not null
"""


@click.command()
@click.argument("db_file", type=click.Path(exists=True))
@click.option("-o", "--output-dir", type=click.Path(exists=True, file_okay=False))
def main(db_file, output_dir):
    with sqlite3.connect(db_file) as conn:
        conn.row_factory = sqlite3.Row
        books = {book["ContentID"]: book for book in query(conn, BOOKS)}
        annotations = query(conn, ANNOTATIONS)

    env = Environment(
        loader=FileSystemLoader("."),
        autoescape=True,
    )
    env.filters["html_to_markdown"] = html_to_markdown
    book_template = env.get_template("book.md.j2")
    annotations_template = env.get_template("annotations.md.j2")

    import_date = datetime.now()

    with status_db_connection(output_dir) as status_db:
        annotations = filter(partial(is_already_imported, status_db), annotations)
        for content_id, annotations in group_by_key(annotations, "VolumeID"):
            book = books[content_id]
            print(f"Importing {len(annotations)} new annotations for {book['Title']}")
            book_filename = output_filename(output_dir, book)

            if not book_filename.exists():
                render_to_file(book_template, book_filename, book=book)

            annotated_days = itertools.groupby(annotations, annotation_date)

            render_to_file(
                annotations_template,
                book_filename,
                annotated_days=annotated_days,
                import_date=import_date,
            )

            save_as_imported(status_db, import_date, annotations)


def render_to_file(template, filename, **kwargs):
    output = template.render(**kwargs)
    with open(filename, "a") as output_file:
        output_file.write(output)


def html_to_markdown(html):
    return html2text.HTML2Text().handle(html)


def annotation_date(annotation):
    return datetime.strptime(annotation["DateCreated"], "%Y-%m-%dT%H:%M:%S.%f").date()


def group_by_key(annotations, key):
    result = defaultdict(list)
    for annotation in annotations:
        result[annotation[key]].append(annotation)
    return result.items()


def query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def output_filename(output_dir, book):
    return Path(output_dir) / f"{book['Title']}.md"


STATUS_DB_SCHEMA = "create table imported (bookmark_id, date_imported)"


def init_status_db(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute(STATUS_DB_SCHEMA)


def status_db_connection(output_dir):
    db_path = Path(output_dir) / "kobo-export.sqlite"
    if not db_path.exists():
        init_status_db(db_path)
    return sqlite3.connect(db_path)


def is_already_imported(status_db, annotation):
    cursor = status_db.cursor()
    cursor.execute(
        "select exists(select 1 from imported where bookmark_id = ?)",
        (annotation["BookmarkID"],),
    )
    (exists,) = cursor.fetchone()
    return not exists


def save_as_imported(status_db, import_date, annotations):
    status_db.executemany(
        "insert into imported (bookmark_id, date_imported) values (?, ?)",
        [(annotation["BookmarkID"], import_date) for annotation in annotations],
    )
    status_db.commit()


if __name__ == "__main__":
    main()
