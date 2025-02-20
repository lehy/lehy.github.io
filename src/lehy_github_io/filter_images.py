#!/usr/bin/env python3

import mistletoe
import structlog
import imageio.v3 as iio
import pathlib
import urllib

log = structlog.get_logger("filter_images")


def iter_children(doc):
    todo = [doc]
    while todo:
        x = todo.pop()
        yield x
        children = getattr(x, "children", [])
        if children:
            todo += children


def is_image(x: str):
    try:
        iio.imread(x)
        return True
    except Exception as e:
        # log.warning("not a readable image", file=x)
        return False


def file_exists(x: str):
    if str(x).endswith(".jpg"):
        return is_image(x)
    try:
        return pathlib.Path(x).stat().st_size > 0
    except FileNotFoundError:
        return False


def longest_common_prefix(seqs):
    min_len = min(len(x) for x in seqs)
    ref = seqs.pop()
    i = 0
    while i < min_len and all(x[i] == ref[i] for x in seqs):
        i += 1
    return ref[:i]


def guess_base_dir(images):
    images = [pathlib.Path(x).parts for x in images]
    prefix = longest_common_prefix(images)
    assert len(prefix) > 0, (prefix, images)
    return pathlib.Path().joinpath(*prefix)


def find_media_directory():
    return pathlib.Path(sys.argv[0]).parent / ".." / ".." / "media"


def to_media(f):
    return (
        (find_media_directory() / ".." / pathlib.Path(*f.parts[1:]))
        .resolve()
        .relative_to(pathlib.Path.cwd())
    )


def to_markdown_link(f):
    parts = list(f.parts)
    i = parts.index("media")
    return pathlib.Path("/", *parts[i:]).resolve()


def delete_files(files):
    for f in files:
        f.unlink()


def to_md_links(images):
    return set([to_markdown_link(x) for x in images])


def main(argv):
    if len(argv) < 2:
        log.error("usage: filter_images.py <album.md>")
        return

    with open(argv[1], "r") as f:
        doc = mistletoe.Document(f)
    images = {
        pathlib.Path(urllib.parse.unquote(x.src))
        for x in iter_children(doc)
        if isinstance(x, mistletoe.span_token.Image)
    }
    if not images:
        log.error("no images were found in document", doc=argv[1])
        return
    image_dir = guess_base_dir(images)
    log.info("guessed image dir", image_dir=image_dir)

    image_dir = to_media(image_dir)

    if not image_dir.is_dir():
        log.error("image directory does not exist", image_directory=image_dir)
        return
    # XXX this is a mess!
    missing_images = {x for x in images if not file_exists(to_media(x))}
    if missing_images:
        log.error(
            "some images in Markdown are not found",
            missing=[str(x) for x in missing_images],
        )
        return
    log.info("all images in document were found on disk", n=len(images))
    images_in_base_dir = to_md_links({x for x in image_dir.iterdir() if x.is_file()})
    images_to_delete = images_in_base_dir.difference(to_md_links(images))
    # log.debug("images in document", images_in_document=to_md_links(images))
    # log.debug("images in base dir", image_in_base_dir=images_in_base_dir)
    # images_ok = images_in_base_dir.intersection(images)
    # log.info("ok images", images=images_ok)
    to_delete_in_media = [to_media(x) for x in images_to_delete]
    if images_to_delete:
        log.info("there are unused images to delete", n=len(images_to_delete))
        response = input("delete (y/n) ? ")
        if response.lower() in ["y", "o"]:
            delete_files(to_delete_in_media)
    else:
        log.info("no image to delete")


if __name__ == "__main__":
    import sys

    main(sys.argv)
