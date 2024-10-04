#!/usr/bin/env python3

import os
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.transport.requests import AuthorizedSession
import pathlib
import datetime
import urllib.parse
import structlog
import imageio.v3 as iio
import sys
import re

log = structlog.get_logger("album2md")


def get_creds(
    secrets_dir="_secrets_",
    scopes=["https://www.googleapis.com/auth/photoslibrary.readonly"],
):
    secrets_dir = pathlib.Path(secrets_dir)
    secrets_dir.mkdir(parents=True, exist_ok=True)

    creds = None

    token_file = secrets_dir / "token.json"
    client_secret_file = secrets_dir / "client_secret.json"
    if not client_secret_file.is_file():
        log.error(
            "could not find client secret file, install it",
            file=str(client_secret_file),
        )
        sys.exit(1)

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, scopes)

    # This is broken, when the creds expire we get invalid grant and
    # they are not refetched.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, scopes)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(token_file, "w") as token:
            token.write(creds.to_json())
    return creds


def get_session():
    creds = get_creds(secrets_dir=pathlib.Path(sys.argv[0]).parent / ".." / ".." / "_secrets_")
    return AuthorizedSession(creds)


def all_pages(f, params):
    go_on = True
    params = dict(**params)
    pages = []
    while go_on:
        ret = f(params=params)
        keys = set(ret.keys())
        if "nextPageToken" in ret:
            params["pageToken"] = ret["nextPageToken"]
            keys.remove("nextPageToken")
        else:
            go_on = False

        assert len(keys) == 1, keys
        key = keys.pop()
        pages += ret[key]
    return pages


def get_albums(sess):
    def get_one(params):
        res = sess.get("https://photoslibrary.googleapis.com/v1/albums", params=params)
        return res.json()

    albums = all_pages(get_one, dict(pageSize=50))
    return {x.get("title", x["id"]): x["id"] for x in albums}


def list_albums():
    session = get_session()
    albs = get_albums(session)
    log.info("no album passed on command line, will list albums")
    for x in sorted(set(albs.keys()), key=lambda x: x.lower()):
        print("  ", x)


def get_album_by_id(sess, album_id):
    def get_one(params):
        res = sess.post(
            "https://photoslibrary.googleapis.com/v1/mediaItems:search",
            headers={"content-type": "application/json"},
            json=params,
        )
        return res.json()

    return all_pages(get_one, dict(pageSize=100, albumId=album_id))


def album_to_pandas(alb):
    alb = [
        dict(
            id=x["id"],
            baseUrl=x["baseUrl"],
            creationTime=x["mediaMetadata"]["creationTime"],
        )
        for x in alb
    ]
    ret = pd.DataFrame(alb)
    ret["creationTime"] = pd.to_datetime(ret["creationTime"], format="ISO8601")
    return ret


def get_album(sess, album_name):
    albums = get_albums(sess)
    try:
        album_id = albums[album_name]
    except KeyError:
        log.error(
            "could not find album name",
            album_name=album_name,
            known_album_names=set(albums.keys()),
        )
        raise
    return album_to_pandas(get_album_by_id(sess, album_id))


def structure_album(df: pd.DataFrame):
    ret = []
    for day, g in df.groupby(df.creationTime.dt.date):
        day_data = dict(date=day)
        g = g.sort_values("creationTime")
        g["delta"] = g.creationTime.diff()
        g.delta.iat[0] = datetime.timedelta(days=365)
        shots_data = []
        current_shot = None
        for _, row in g.iterrows():
            if row.delta > datetime.timedelta(minutes=10):
                if current_shot is not None:
                    shots_data.append(current_shot)
                current_shot = dict(creationTime=row.creationTime, shots=[])
            current_shot["shots"].append(dict(id=row.id, baseUrl=row.baseUrl))
        shots_data.append(current_shot)
        day_data["scenes"] = shots_data
        ret.append(day_data)
    return ret


def image_file_name(directory, image_id, image_size):
    image_id = urllib.parse.quote(image_id, safe="")
    return pathlib.Path(directory) / (image_id + image_size + ".jpg")


def save_image(
    directory: pathlib.Path, image_id: str, image_size: str, contents: bytes
) -> str:
    image_file = image_file_name(directory, image_id, image_size)
    directory.mkdir(parents=True, exist_ok=True)
    with open(image_file, "wb") as f:
        f.write(contents)
    return image_file


def is_image(x: str):
    try:
        iio.imread(x)
        return True
    except Exception as e:
        # log.warning("not a readable image", file=x)
        return False


def download_image(session, image_directory, image_id, url, image_size):
    image_file = image_file_name(image_directory, image_id, image_size)
    if is_image(image_file):
        log.info("image already exists", image=str(image_file))
        return image_file
    log.info("downloading image", image=str(image_file))
    image = session.get(url + image_size)
    assert image.status_code == 200, (image.status_code, image.url)
    saved_image_file = save_image(image_directory, image_id, image_size, image.content)
    assert saved_image_file == image_file, (saved_image_file, image_file)
    return image_file


def find_media_directory():
    return pathlib.Path(sys.argv[0]).parent / ".." / ".." / "media"


def find_posts_directory():
    return pathlib.Path(sys.argv[0]).parent / ".." / ".." / "_posts"


def make_media_file_name(f):
    parts = list(f.parts)
    i = parts.index("media")
    return pathlib.Path("/", *parts[i:])


def output_markdown(
    session,
    album: pd.DataFrame,
    md_file="article.md",
    image_directory="images",
    image_size="=w800-h800",
    max_days=None,
):
    grouped = structure_album(album)
    if max_days is not None:
        grouped = grouped[:max_days]

    media_directory = find_media_directory()
    posts_directory = find_posts_directory()

    image_directory = pathlib.Path(image_directory)
    log.info("using image directory", image_directory=image_directory)

    with open(posts_directory / md_file, "w") as out:
        for day in grouped:
            day_date = day["date"]
            day_date_s = day_date.strftime("%A %d %B %Y")
            out.write(f"\n## {day_date_s}\n")
            for scene in day["scenes"]:
                scene_date = scene["creationTime"]
                scene_date_s = scene_date.strftime("%H:%M")
                out.write(f"\n### {scene_date_s}\n")

                for shot in scene["shots"]:
                    image_file = download_image(
                        session,
                        media_directory / image_directory,
                        shot["id"],
                        shot["baseUrl"],
                        image_size,
                    )
                    # The link should point to /media/image_directory/12345.jpg
                    # while the image was saved to something like "../media/image_directory/12345.jpg"
                    image_file = make_media_file_name(image_file)
                    encoded_image_file = urllib.parse.quote(str(image_file), safe="/")
                    out.write(f"![]({encoded_image_file}) ")


def output_article(album_name):
    session = get_session()
    album = get_album(session, album_name)
    # filter out non ascii, it is annoying in the terminal and causes
    # other problems, like Emacs that won't open an image when there
    # is an accent in the path
    album_name = re.sub(r"[^a-zA-Z0-9_-]+", "-", album_name)
    today = datetime.datetime.now().date().isoformat()
    article_name = f"{today}-{album_name}.md"
    image_directory = f"{album_name}-images"
    if pathlib.Path(article_name).is_file():
        log.error("article already exists", file=article_name)
        return
    output_markdown(
        session, album, md_file=article_name, image_directory=image_directory
    )
    log.info("article was written", article_file_name=article_name)


def main(argv):
    if len(argv) > 1:
        album_name = argv[1]
        output_article(album_name)
    else:
        list_albums()


if __name__ == "__main__":
    import sys

    main(sys.argv)
