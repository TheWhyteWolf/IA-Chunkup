# ia_upload

A drop-in replacement for `ia upload` for large uploads to archive.org.

## Why

Uploading anything sizeable through the `internetarchive` library tends to die
like this:

```
requests.exceptions.ReadTimeout: (ReadTimeoutError("HTTPSConnectionPool(
host='s3.us.archive.org', port=443): Read timed out. (read timeout=120)"),
'https://s3.us.archive.org/Game-maps-collection/.../rectf012.zip')
```

The 120 is hardcoded in the library, in `upload_file`:

```python
if 'timeout' not in request_kwargs:
    request_kwargs['timeout'] = 120
```

It is a *read* timeout, so it limits how long the client waits for IA's reply,
which only begins once the whole file is already on the wire. IA writes the
object to a storage node and checksums it before answering, so a large zip runs
past 120s while the transfer is working perfectly.

Two consequences worth knowing:

* This is not a bandwidth problem, and it is not fixed by chunking. `requests`
  already streams the body off disk in small blocks and never holds the file in
  memory. There is nothing left to chunk on the client side.
* Raising `--retries` does not help either. The library's retry loop only fires
  on HTTP 503 and on its "S3 overloaded" pre-check. A `ReadTimeout` is raised
  straight out and never retried, at any retry count.

The `ia` command line exposes no timeout option, so none of this can be fixed
with a flag. Hence this script.

## Install

```sh
pip install internetarchive
ia configure          # once, for your archive.org login
chmod +x ia_upload.py
```

Python 3.9 or newer. No dependencies beyond `internetarchive`.

## Usage

Dry run first. It reads the item but sends nothing, and needs no credentials:

```sh
./ia_upload.py Game-maps-collection ./maps/ --dry-run
```

Then the real thing:

```sh
./ia_upload.py Game-maps-collection ./maps/ --derive --log upload.log
```

If it dies, gets killed, or you Ctrl-C it, run the identical command again. It
re-reads the item, compares MD5s, and sends only what is missing. Nothing is
ever uploaded twice.

Creating a new item needs metadata on the first run, or IA will usually reject
the upload as spam:

```sh
./ia_upload.py Game-maps-collection ./maps/ \
    -m 'title:Game Maps Collection' \
    -m 'description:Community map packs, 1999-2007.' \
    -m 'mediatype:software' \
    --derive --log upload.log
```

## Coming from `ia upload`

```sh
ia upload ID DIR --checksum --verify --retries 150 --sleep 15
```

becomes

```sh
./ia_upload.py ID DIR
```

| `ia upload` | here |
|---|---|
| `--checksum` | Default. Reads the item's file list once per run rather than once per file, and caches local MD5s between runs. |
| `--verify` | Always on. Every request carries a `Content-MD5` computed from the cached hash, so IA verifies server-side without re-reading the file on each attempt. |
| `--retries 150` | `--retries 12` by default, but these retries cover timeouts and dropped connections, and back off exponentially rather than sleeping a flat interval. |
| `--sleep 15` | `--retry-wait 15` by default, doubling up to `--max-retry-wait 600`. |

The read timeout scales with file size: 600s plus 300s per GB, capped at two
hours. A 4 GB map pack gets half an hour to be acknowledged instead of 120
seconds.

## What it adds

* Read timeout scaled to file size instead of a fixed 120s.
* On a timeout, re-reads the item's file list to check whether the upload
  landed anyway before spending another full transfer on it. Timeouts on IA are
  more often a lost acknowledgement than a lost transfer.
* Retries timeouts, connection resets, 429 and 5xx, with exponential backoff
  and jitter. Stops immediately on 401/403 instead of retrying 150 times
  against a credentials problem.
* Checks every filename for what IA rejects (control characters, over-long
  names, space-padded path segments, case-insensitive collisions) before
  starting rather than four hours in.
* Confirms every uploaded file's MD5 against the item at the end.
* Progress, throughput and ETA, plus a heartbeat during long single-file
  transfers so an unattended run is distinguishable from a hung one.
* Never follows symlinks. Prompts before overwriting a file whose contents
  differ, since IA keeps the old copy and it still counts against the item.
* Redacts anything shaped like an S3 credential from logs.

## Documentation

```sh
man -l ia_upload.1
```

Or install it:

```sh
mkdir -p ~/.local/share/man/man1
cp ia_upload.1 ~/.local/share/man/man1/
man ia_upload
```

## Exit codes

| code | meaning |
|---|---|
| 0 | everything uploaded and verified, or nothing to do |
| 1 | some files failed; re-run the same command to retry just those |
| 2 | bad usage, bad identifier, missing credentials, or a filename IA will reject |
| 130 | interrupted; re-run to continue |

## Credentials

`ia configure` is the easy path. If you prefer environment variables the names
are `IA_ACCESS_KEY_ID` and `IA_SECRET_ACCESS_KEY`. Set both or neither, since
the library raises if only one is present.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
