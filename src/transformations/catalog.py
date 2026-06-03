def normalize_artist_name(artist_name):
    if artist_name is None:
        return None
    name = artist_name.strip()
    splitted_name = name.split()
    name = [word.capitalize() for word in splitted_name]
    return " ".join(name)


def validate_track_schema(track: dict) -> list:
    errors = []
    print(track)
    if "title" not in track:
        errors.append("title")
    if (
        "duration_ms" in track
        and track["duration_ms"] < 0
        or track["duration_ms"] > 36_000_000
    ):
        errors.append("duration")
    return errors


def deduplicate_artists(artists):
    """Return a list of unique artists.
    Two artists are considered duplicates if their **normalized** names are equal **and** they belong to the same label.
    The first occurrence is kept; later duplicates are discarded.
    The function returns a list of dicts with keys ``id``, ``name`` (normalized) and ``label``.
    """
    unique_map = {}
    for artist in artists:
        name = normalize_artist_name(artist.get("name"))
        label = artist.get("label")
        key = (name, label)
        if key not in unique_map:
            unique_map[key] = {
                "id": artist.get("id"),
                "name": name,
                "label": label,
            }
    return list(unique_map.values())

