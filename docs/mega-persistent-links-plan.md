# MEGA public links and remote-only library records

## Goal

After ReClip uploads a local video to MEGA:

1. ReClip automatically creates a public `https://mega.nz/...` link for the uploaded file.
2. The link is stored against the corresponding ReClip library item, independently of the temporary upload-job history.
3. The library card and transfer row expose an **Open in MEGA** button.
4. Deleting a MEGA-backed video removes only its local media file. Its metadata, thumbnail, and MEGA link remain visible in the library.
5. ReClip never deletes the remote MEGA object as part of local deletion.

A MEGA public link cannot be created before the remote object exists. During transfer the UI will continue showing progress, then briefly show a `creating link` state; the button becomes available immediately after MEGA returns the link.

## Current behavior and gaps

- `mega_helper.py` ends a successful `rclone copyto` job without calling `rclone link`, so upload jobs have a remote path but no browser-openable URL.
- MEGA upload history is kept in `.mega/uploads.json`, is capped, and can be cleared. It is therefore not suitable as the authoritative video-to-link store.
- The Downloads library is backed by SQLite, but `/api/library` skips rows whose local files are gone.
- Both deletion paths (`delete_job()` and `/api/library/delete`) unregister the SQLite rows before/while deleting the local file.
- The current red MEGA badge is inferred from upload jobs and browser `localStorage`; it is not backed by a saved server-side link.
- Selection and delete APIs identify a file mainly by filename. The group id must also be sent to avoid ambiguity between groups.

## Design

### 1. Persist remote state in the download record

Add backward-compatible columns to `downloads` in `app.py`:

- `size_bytes INTEGER` — retained size for a card after the local file is removed.
- `local_deleted_at REAL` — explicit local-removal marker.
- `mega_url TEXT` — public MEGA capability URL.
- `mega_remote_path TEXT` — remote path used by rclone.
- `mega_account_id TEXT` and `mega_account_label TEXT` — account snapshot for diagnostics/display.
- `mega_uploaded_at REAL` — successful link/upload time.

This is intentionally one current MEGA copy per library item, matching the requested single MEGA button. A later successful re-upload may replace the stored link with the newest copy; it must never delete the older remote object.

Update `_DB_SCHEMA`, additive migrations in `_init_db()`, `_row_to_entry()`, `load_index()`, and `_save_index_locked()`. Use explicit column lists for writes rather than positional `INSERT ... VALUES` so future migrations are safer.

The dedup index can contain multiple keys for one physical file. Any MEGA/local-state update must update every entry that points to the resolved source path. On registration/redownload, merge and preserve existing MEGA fields instead of replacing them.

### 2. Generate a public link after upload

Extend `MegaHelper` with an `on_link_ready` callback supplied by `app.py`.

Successful worker flow:

1. Run the existing `rclone copyto` command.
2. Mark the job `linking` (an active state).
3. Run:

   ```text
   rclone link <remote>:<remote_path> --config <config>
   ```

4. Read the last non-empty output line, validate that it is an HTTPS MEGA URL, and store it as `public_url` in `.mega/uploads.json`.
5. Invoke `on_link_ready` with the private source path plus the public job metadata. The callback updates the matching in-memory download entries and SQLite rows.
6. Mark the job `done` only after both the URL and its library association have been persisted.

If the copy succeeds but link creation fails, retain `remote_uploaded: true`, set a clear error such as “Upload completed, but the public link could not be created,” and do not claim the item is safely MEGA-backed in the library.

Add a retry route (`POST /api/mega/uploads/<id>/link`) that creates/recreates only the link without uploading the media again. This also gives existing completed jobs without `public_url` an explicit migration path, without silently publishing every historical upload on startup.

On startup, reconcile retained jobs that already contain `public_url` into SQLite. This closes the small crash window between the two persistence files. Clearing finished transfer rows must not clear links from the library.

### 3. Make deletion local-only for MEGA-backed items

Create one shared local-deletion helper and use it from both `delete_job()` and `/api/library/delete`.

For an item with `mega_url`:

- save its current size before removal;
- remove only the media file from disk;
- retain all download-index/SQLite entries and metadata;
- set `local_deleted_at`;
- keep the thumbnail so the remote-only card remains recognizable;
- clear stale live-job file references as needed;
- never call an rclone delete/unlink operation.

For an item without a saved MEGA URL, preserve the existing hard-delete behavior.

Deletion should return `409` while the same file has a queued, uploading, or linking MEGA job. This prevents the source disappearing before a verified link is persisted.

`find_existing_download()` must continue to return only an existing local file for deduplication, but it must no longer prune a missing row that has `mega_url`. `/api/library` should include such rows with:

- `local_available: false`
- persisted `size`
- the MEGA fields

A remote-only item is history, not a valid local dedup hit and not eligible for another upload until it is downloaded locally again.

Group deletion must also reassign remote-only records to Ungrouped even though there is no local file to move.

### 4. Expose the saved link in the UI

In `templates/index.html` (and keep `/legacy` behavior compatible):

- Remove the `localStorage`/finished-job-derived MEGA badge as the source of truth.
- Read `mega_url` and `local_available` directly from `/api/library`.
- Add a red **Open in MEGA** action to every linked library card.
- Add the same open action to completed transfer rows.
- Show `MEGA only` on cards whose local media has been removed.
- For remote-only cards, hide/disable local download and MEGA-upload selection controls.
- Change the linked-item confirmation to “Delete the local copy? The MEGA file and link will be kept.”
- Send both `filename` and `group_id` to the delete endpoint; retain backwards compatibility for older clients.
- Refresh/patch the library once a polled upload moves to `done`, so the button appears without a page reload.
- Open public URLs with a new tab and `noopener,noreferrer`.

Library disk-usage totals should count only `local_available` items, while each remote-only card may still display its original file size.

### 5. Tests

#### `tests/test_mega_helper.py`

- Fake rclone supports `link` and returns a representative `https://mega.nz/file/...#...` URL.
- A successful upload transitions through link creation and persists `public_url`.
- The completion callback receives the right source/group/remote metadata.
- Link failure reports that the copy succeeded but does not mark the job `done`.
- Link-only retry succeeds without a second `copyto`.
- Reloading helper state retains the public URL.

#### `tests/test_app.py`

- Schema migration/load/save retains all new columns.
- A completed link is attached to every dedup key for the physical file.
- `/api/library` exposes `mega_url` and `local_available`.
- Deleting a linked item removes the local file but keeps the SQLite row, thumbnail metadata, size, and link across reloads.
- Deleting an unlinked item still removes its record.
- A remote-only record is visible but is not considered a local dedup hit or upload source.
- Active MEGA transfers block local deletion.
- Clearing finished transfer history does not remove the library link.
- Same filenames in different groups update/delete only the selected item.

## Implementation order

1. Add SQLite fields, migration, and index serialization.
2. Add the MEGA link command, job state, callback, retry route, and startup reconciliation.
3. Centralize local deletion and make library serialization remote-aware.
4. Update current and legacy UI actions/states.
5. Add tests, then document the public-link behavior and privacy implication in `README.md`.

## Acceptance checks

1. Upload a video and watch `queued -> uploading -> linking -> done`.
2. Open the generated MEGA URL from both the transfer row and library card.
3. Clear finished transfer rows; the card’s MEGA button still works.
4. Delete the card’s local copy; disk usage falls, the card remains with `MEGA only`, and its button still opens the same URL.
5. Restart ReClip; the remote-only card and URL remain.
6. Confirm no local-delete flow invokes a remote MEGA delete or public-link unlink.

## Security note

A MEGA public URL contains the capability needed to access/decrypt that file. It should be treated as sensitive. ReClip will create it only as part of a requested upload (or an explicit retry for an old upload), return it only through the existing ReClip API, and will not log it unnecessarily.
