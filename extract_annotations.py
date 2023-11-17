from collections import defaultdict
from datetime import datetime
import itertools
from pathlib import Path
import click
import sqlite3
from jinja2 import Environment, FileSystemLoader, select_autoescape

ANNOTATIONS = """
    select
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


def output_path(book):
    return Path("output") / f"{book['Title']}.md"


@click.command()
@click.argument("db_file", type=click.Path(exists=True))
def main(db_file):
    with sqlite3.connect(db_file) as conn:
        conn.row_factory = sqlite3.Row
        books = {book["ContentID"]: book for book in query(conn, BOOKS)}
        annotations = query(conn, ANNOTATIONS)

    env = Environment(
        loader=FileSystemLoader("."),
        autoescape=select_autoescape(["html", "md"]),
    )
    template = env.get_template("book.md.j2")

    for content_id, annotations in group_by_key(annotations, "VolumeID"):
        book = books[content_id]

        annotated_days = itertools.groupby(annotations, annotation_date)

        output = template.render(
            book=book,
            annotated_days=annotated_days,
        )
        with open(output_path(book), "w") as output_file:
            output_file.write(output)


if __name__ == "__main__":
    main()
