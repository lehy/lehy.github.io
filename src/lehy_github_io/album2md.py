#!/usr/bin/env python3

import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.transport.requests import AuthorizedSession
import google.auth.exceptions
import pathlib
import datetime
import urllib.parse
import structlog
import textwrap
import imageio.v3 as iio
from tqdm import tqdm
import sys
import re

log = structlog.get_logger("album2md")

MAX_DOWNLOAD_SIZE_BYTES = 10 * (2**20)


def download(session, url, output_path, chunk_size_bytes=1024):
    # Determine the size of the partially downloaded file, if it exists
    downloaded_size = 0
    output_path = pathlib.Path(output_path)
    if output_path.exists():
        downloaded_size = output_path.stat().st_size
        log.info("resuming download", already_downloaded_bytes=downloaded_size)

    headers = {"Range": f"bytes={downloaded_size}-"}

    # Make the request with streaming enabled
    with session.get(url, headers=headers, stream=True) as response:
        # If the server does not support 'Range', start from the beginning
        if response.status_code not in (206, 200):  # 206 means "Partial Content"
            log.info(
                "server does not support resuming, restarting download",
                status_code=response.status_code,
            )
            downloaded_size = 0  # Reset download size
            open(output_path, "wb").close()  # Truncate the file
            headers.pop("Range")

        # Get the total file size from the 'Content-Range' or 'Content-Length' header
        total_size = int(response.headers.get("Content-Length", 0))
        if "Content-Range" in response.headers:
            total_size += downloaded_size
        assert total_size >= downloaded_size, (total_size, downloaded_size)

        if total_size > MAX_DOWNLOAD_SIZE_BYTES:
            log.warning("not downloading big file", total_size=total_size)
            return None

        # Open the file in append mode to continue writing from the last position
        with output_path.open("ab") as file, tqdm(
            desc=f"downloading {output_path.name}",
            total=total_size,
            initial=downloaded_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar:
            # Read the response in chunks and write to the file
            for chunk in response.iter_content(chunk_size=chunk_size_bytes):
                if chunk:  # Only write non-empty chunks
                    file.write(chunk)
                    progress_bar.update(len(chunk))
            return output_path


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

    if token_file.exists():
        creds = Credentials.from_authorized_user_file(token_file, scopes)

    # This is broken, when the creds expire we get invalid grant and
    # they are not refetched.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except google.auth.exceptions.RefreshError:
                # If the refresh fails, force a new authentication
                token_file.unlink(missing_ok=True)  # Delete the old token
                creds = None
        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_file, "w") as token:
            token.write(creds.to_json())
    return creds


def get_session():
    creds = get_creds(
        secrets_dir=pathlib.Path(sys.argv[0]).parent / ".." / ".." / "_secrets_"
    )
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


def canon_rel_path(path: pathlib.Path):
    return (
        pathlib.Path(*path.parts)
        .resolve(strict=False)
        .relative_to(pathlib.Path().resolve())
    )


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
    # log.debug("album", alb=alb)
    alb = [
        dict(
            id=x["id"],
            baseUrl=x["baseUrl"],
            creationTime=x["mediaMetadata"]["creationTime"],
            mimeType=x["mimeType"],
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
            known_album_names=sorted(set(albums.keys())),
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
            current_shot["shots"].append(
                dict(id=row.id, baseUrl=row.baseUrl, mimeType=row.mimeType)
            )
        shots_data.append(current_shot)
        day_data["scenes"] = shots_data
        ret.append(day_data)
    return ret


class UnknownMimeType(Exception):
    pass


def image_file_name(directory, image_id, mime_type, image_size):
    image_id = urllib.parse.quote(image_id, safe="")
    if mime_type == "video/mp4":
        ext = "-dv.mp4"
    elif mime_type == "image/jpeg":
        ext = image_size + ".jpg"
    else:
        log.error("unknown mime type", mime_type=mime_type)
        raise UnknownMimeType(mime_type)
    return canon_rel_path(pathlib.Path(directory) / (image_id + ext))


# def save_image(
#     directory: pathlib.Path,
#     image_id: str,
#     mime_type: str,
#     image_size: str,
#     contents: bytes,
# ) -> str:
#     image_file = image_file_name(directory, image_id, mime_type, image_size)
#     directory.mkdir(parents=True, exist_ok=True)
#     with open(image_file, "wb") as f:
#         f.write(contents)
#     return image_file


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


def download_image(
    session, image_directory: pathlib.Path, image_id, url, mime_type, image_size
):
    image_directory.mkdir(parents=True, exist_ok=True)
    image_file = image_file_name(image_directory, image_id, mime_type, image_size)
    if file_exists(image_file):
        log.info("file already exists", image=str(image_file))
        return image_file
    image_file_temp = image_file.with_name(image_file.name + ".temp")
    log.info(
        "downloading image/video",
        image_temp=str(image_file_temp),
        image_file=image_file,
    )
    if mime_type == "video/mp4":
        url = url + "=dv"
        # image = session.get(url + '=dv')
    elif mime_type == "image/jpeg":
        url = url + image_size
        # image = session.get(url + image_size)
    else:
        assert False, mime_type
    temp_file = download(session, url, image_file_temp)
    if temp_file is None:
        return None
    temp_file.rename(image_file)
    # assert image.status_code == 200, (image.status_code, image.url)
    # saved_image_file = save_image(
    #     image_directory, image_id, mime_type, image_size, image.content
    # )
    # assert saved_image_file == image_file, (saved_image_file, image_file)
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

    image_directory = pathlib.Path(image_directory)
    log.info("using image directory", image_directory=image_directory)

    with open(md_file, "w") as out:
        out.write(
            textwrap.dedent("""
        ---
        layout: post
        title: <titre>
        ---
        
        """)
        )

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
                        shot["mimeType"],
                        image_size,
                    )
                    if image_file is None:
                        out.write(f"<!-- skipped big image {shot['id']}--!>\n")
                        continue
                    # The link should point to /media/image_directory/12345.jpg
                    # while the image was saved to something like "../media/image_directory/12345.jpg"
                    image_file = make_media_file_name(image_file)
                    encoded_image_file = urllib.parse.quote(str(image_file), safe="/")
                    if shot["mimeType"] == "image/jpeg":
                        out.write(f"![]({encoded_image_file}) ")
                    elif shot["mimeType"] == "video/mp4":
                        out.write(
                            f'<video controls width="100%"><source src="{encoded_image_file}" type="video/mp4" /></video>'
                        )


def output_article(album_name):
    session = get_session()
    album = get_album(session, album_name)
    # filter out non ascii, it is annoying in the terminal and causes
    # other problems, like Emacs that won't open an image when there
    # is an accent in the path
    album_name = re.sub(r"[^a-zA-Z0-9_-]+", "-", album_name)
    today = datetime.datetime.now().date().isoformat()
    posts_directory = find_posts_directory()
    article_name = posts_directory / f"{today}-{album_name}.md"
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
