# Video group reassignment and library multi-selection

## Goal

Let a user reorganize completed library items after download:

1. Move one video to another existing group or to **Ungrouped**.
2. Select multiple library items and move them to one destination in a single action.
3. Keep the filesystem, in-memory download index, SQLite data, live job references, group counts, and UI in sync.
4. Preserve MEGA links and remote-only history introduced by `docs/mega-persistent-links-plan.md`.

A ReClip group is both library metadata and, for a locally available item, a directory under `DOWNLOAD_DIR`. Changing a group therefore moves the local file; it is not only a label change.

## Scope and UX

### Shared library selection

The current Downloads selection is described and styled as MEGA-only. Generalize it into one library selection model rather than adding a second competing set of checkboxes:

- Each library card has a visible checkbox and a neutral selected state.
- Clicking a card outside its links/actions toggles it, as today.
- The existing select-all control selects or deselects the currently filtered cards only.
- Selection may span search/filter changes and is keyed by the existing exact identity `[group_id, filename]`.
- The sticky action bar shows the selected count and size, then **Move to…**, **Send to MEGA**, and **Clear**.
- Add a small **Move** action to each card for the discoverable single-item flow; it opens the same destination picker for only that card.

The destination picker lists **Ungrouped** followed by the named groups. Choosing the item's current group is allowed and produces a no-op. Group creation remains in the existing group manager and is not duplicated in this picker.

All library records can be selected for moving, including a MEGA-only record. **Send to MEGA** remains available only when every selected item has a local copy; otherwise it is disabled with a “Local copy required” explanation. MEGA keeps its red branding, while generic selection and move controls use the normal ReClip colors.

While a move request is running, disable the selection controls and show a `Moving…` state. Do not optimistically patch paths because the server may rename a file to avoid a destination collision. On success, clear the moved selection, refresh `/api/library` and `/api/groups`, and keep the current filters. Items moved out of the currently filtered group naturally disappear.

Apply the same behavior to `templates/index.html` and `templates/legacy.html`. The legacy UI may use its native select and checkbox styling, but it must use the same endpoint and semantics.

## Current behavior and gaps

- A group can be chosen only before a download starts.
- Group ids double as folder names; renaming a group changes only its display name.
- Deleting a group moves local files to the download root, but there is no reusable move primitive for an individual item.
- The download index can contain several dedup keys pointing to one physical file. Updating one row would leave the others stale.
- Finished and deduplicated in-memory jobs can retain the old file path after a filesystem move.
- Library operations historically used filename alone. This is ambiguous when two groups contain the same filename; reassignment must require both source `group_id` and `filename`.
- The existing selection state and action bar are coupled to MEGA uploads even though they already implement most of the required multi-selection behavior.
- A queued/uploading/linking MEGA job retains the source path and source group. Moving that file concurrently can break the upload or its public-link callback.
- On this branch, a MEGA-backed record may remain in the library after its local file is deleted. It must still be logically movable without pretending that the MEGA object moved.

## API

Add one batch-capable route:

```text
PATCH /api/library/group
Content-Type: application/json

{
  "files": [
    {"filename": "clip.mp4", "group_id": "source-group"},
    {"filename": "other.mp4", "group_id": ""}
  ],
  "target_group_id": "archive"
}
```

Rules:

- `files` must be a non-empty list of at most 500 objects.
- `filename` must be a basename, and `group_id` must be present even when it is the empty string for Ungrouped.
- Duplicate source identities are removed while preserving request order.
- Empty `target_group_id` means Ungrouped; any non-empty target must exist.
- Every source is resolved by the exact `(group_id, filename)` pair. Zero matches returns `404`; an ambiguous match returns `409` rather than moving an arbitrary item.
- Validation is all-or-nothing. A missing item, invalid target, active MEGA transfer, or failed filesystem preflight leaves the whole request unchanged.
- Selecting an item already in the target is reported as unchanged.

A successful response returns authoritative post-move identities because collision handling can change filenames:

```json
{
  "ok": true,
  "target_group_id": "archive",
  "moved_count": 2,
  "unchanged_count": 0,
  "files": [
    {
      "from": {"filename": "clip.mp4", "group_id": "source-group"},
      "to": {"filename": "clip (2).mp4", "group_id": "archive"}
    }
  ]
}
```

Use clear status codes: `400` for malformed input, `404` for a missing source/target, `409` for ambiguity or an active conflicting operation, and `500` for a filesystem/persistence failure. The UI keeps the selection on failure so the user can retry.

No schema migration is required for this feature. The existing `(group_id, filename)` identity is sufficient because files within one local group already receive unique names. The move implementation must extend that uniqueness rule to remote-only records as well.

## Backend design

### 1. Centralize library identity resolution

Add a helper that resolves exact selection objects into unique library records while accounting for duplicate dedup-index rows. It should:

- validate basename-only filenames;
- match both source group and filename;
- collapse all index entries sharing the same historical/physical `file` path into one item;
- return every index key/entry that must be updated for that item;
- distinguish a locally available item from a retained remote-only item;
- reject ambiguous identities instead of choosing the first match.

Use this exact resolver in the new route. Where practical, also reuse it in library download, thumbnail, delete, and MEGA selection paths so identical filenames in different groups consistently target the requested record. Keep filename-only fallback only where an older client requires compatibility; the new move API must never use it.

### 2. Plan destinations without overwriting

For a local item, map the target to:

- `DOWNLOAD_DIR/<target_group_id>/<filename>` for a named group;
- `DOWNLOAD_DIR/<filename>` for Ungrouped.

Create the named-group directory if necessary. Reuse the existing numbered-name convention (`name (2).ext`, `name (3).ext`, …), but reserve names against all of the following before moving anything:

- files already on disk in the target directory;
- retained remote-only library identities in the target group;
- destinations assigned to earlier items in the same batch.

Never overwrite an existing file. Preserve the full extension behavior used by `store_final_file()`, including compound/archive filenames.

A remote-only item has no media file to rename. Update its logical `group_id`; keep its historical source path for MEGA-link reconciliation. If its filename would collide with another target identity, assign the same numbered logical filename. This renames only the ReClip library identity, not the remote MEGA object.

### 3. Execute as one library mutation

Introduce one application-level library mutation lock and use it for group moves, local deletion, group deletion, and MEGA upload source reservation. This closes the race where MEGA resolves a path, a group move renames it, and the upload is then queued with the stale path.

Under the mutation:

1. Validate the target and resolve every source.
2. Reject the request if any local source has a MEGA job in `queued`, `uploading`, or `linking` state.
3. Build and reserve all destination names before changing the filesystem.
4. Move local files with `os.rename` while holding the existing rename guard.
5. If a move fails, roll back earlier filesystem moves in reverse order and leave index/job metadata unchanged.
6. Update every `download_index` entry that points to each old path: `file`, `filename`, and `group_id`. Preserve dedup keys and all descriptive/MEGA metadata.
7. Update every in-memory download job that points to an old path so `/api/file/<job_id>`, deduped cards, and later deletion use the new path and filename. Set its `group_id` to the target as well.
8. Persist the index once after the complete batch. This path must surface persistence errors rather than silently swallowing them; on failure restore the in-memory snapshot and roll back filesystem moves.

Do not move thumbnails. They live in the shared thumbnails directory and remain associated through the download entries.

### 4. Reuse the primitive for group deletion

Refactor `DELETE /api/groups/<gid>` to collect that group's records and invoke the same move planner with an Ungrouped target before removing the group metadata/directory. This gives group deletion the same collision protection, dedup-row updates, active-upload guard, rollback behavior, and remote-only handling.

Remove the group only after all items have moved successfully. A failed move leaves the group intact. Empty source directories can be removed after commit; ordinary group reassignment should leave an empty group folder available for future downloads.

### 5. Preserve MEGA semantics

A group reassignment changes ReClip organization only:

- keep `mega_url`, account metadata, upload timestamp, and `mega_remote_path` unchanged;
- never call `rclone moveto`, `delete`, `link`, or `unlink`;
- do not rewrite completed MEGA transfer history, because its group and remote path describe where that upload actually went;
- block reassignment while the upload/link callback still depends on the current local identity;
- allow remote-only linked records to move logically.

Serialize MEGA enqueue source resolution/job registration with the library mutation lock. The result must be deterministic: either the upload is registered first and the move returns `409`, or the move finishes first and an upload request carrying the old identity returns `404`.

## Frontend implementation

### `templates/index.html`

- Rename the JS selection concept from `megaSelected` to a generic library selection set and keep the `[group_id, filename]` key helper.
- Replace MEGA-specific card labels (`Select … for MEGA`) and red selected-card styling with generic accessible selection labels/styling.
- Generalize the filtered select-all button and its `none / partial / all` state.
- Expand the sticky action bar with **Move to…** and keep **Send to MEGA** as one consumer of the shared selection.
- Add a reusable destination popover/menu populated from `allGroups`, including Ungrouped.
- Add the per-card Move action and stop its click from toggling the card twice.
- Submit exact source identities and the chosen target to `PATCH /api/library/group`.
- Disable controls during submission; on success clear the affected keys and refresh library/group state; on failure show the API error and retain selection.
- Ensure download, thumbnail, delete, and MEGA requests continue sending the card's group id after a move or collision rename.

### `templates/legacy.html`

- Generalize the existing MEGA checkbox set and selection bar.
- Add a destination `<select>` and **Move selected** action using `allGroups`.
- Keep **Send to MEGA** in the same bar and apply the same local-availability rule.
- Send source group ids for every selected item and refresh both group counts and the library after success.

## Tests

### `tests/test_app.py`

Add coverage for:

- one local file moved group-to-group, group-to-Ungrouped, and Ungrouped-to-group;
- a bulk move from mixed source groups with one SQLite save and correct response mappings;
- persistence across `load_index()` and correct `/api/library` group data after restart;
- all dedup keys and all in-memory job references following the new path;
- same filenames in different source groups moving only the selected identity;
- an occupied destination receiving a numbered filename without changing/overwriting the existing file;
- two same-named items in one batch receiving distinct planned destinations;
- malformed selection, missing source, missing target, ambiguous identity, duplicate input, and same-group no-op behavior;
- a simulated failure on the second rename rolling the first move back and leaving metadata unchanged;
- queued, uploading, and linking MEGA jobs returning `409` without any partial move;
- a MEGA-backed remote-only item changing groups while retaining its link, remote path, size, and absent-local state;
- group deletion using the same collision-safe path and reassigning both local and remote-only records before deleting the group;
- download, thumbnail, delete, and MEGA resolution still targeting the correct item after a move and collision rename.

Keep the existing served-template smoke test and assert that both UIs expose the move controls. There is no frontend test harness today, so exercise detailed selection/filter behavior in the manual acceptance pass.

## Implementation order

1. Add exact library-item resolution and collision-aware destination planning.
2. Add the mutation lock, atomic move helper, rollback, index persistence, and job-reference updates.
3. Add `PATCH /api/library/group` and refactor group deletion to use the helper.
4. Serialize MEGA enqueue with the mutation and cover remote-only records.
5. Generalize selection and add move controls in the current UI.
6. Add the compatible legacy controls.
7. Add backend/template tests and document reassignment in `README.md`.

## Acceptance checks

1. Move one local video between two groups; verify the file changes directories and survives restart in the new group.
2. Select filtered videos from multiple groups, move them together, and confirm group counts and the filtered list update immediately.
3. Move a file into a group that already contains its name; confirm both files remain and the moved file gets a numbered name.
4. Select two same-named files from different groups and move them together without one overwriting or hiding the other.
5. Start a MEGA upload and confirm moving its source is blocked until `linking` finishes.
6. Move a linked local item and confirm its saved MEGA button still opens the same URL and its recorded remote path does not change.
7. Delete that local copy, move the resulting MEGA-only card again, restart ReClip, and confirm the card remains in the new group.
8. Force a filesystem move failure in a batch and confirm no item is left partially reassigned.
9. Delete a populated group and confirm all records are safely reassigned to Ungrouped with collisions handled and no MEGA object changed.

## Non-goals

- Creating a new group from inside the move picker.
- Moving or renaming files already stored in MEGA.
- Changing the destination group of downloads that are still queued or running.
- Drag-and-drop between group filters; it can be added later on top of the same endpoint.
