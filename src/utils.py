import re


def natural_key(text: str) -> list:
    """
    Natural sort key: d2 < d10, article_2 < article_10.
    Works for any document naming scheme.
    """

    return [
        int(part) if part.isdigit() else part
        for part in re.split(r"(\d+)", str(text))
    ]
