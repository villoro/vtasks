import re


def to_snake_case(name):
    """
    Transforms a name in CamelCase, KebabCase (or any ugly combination)
    into the beautifiul snake_case
    """

    # Handle kebab-case
    out = name.replace("-", "_")

    # Handle CamelCase
    out = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", out)
    out = re.sub("([a-z0-9])([A-Z])", r"\1_\2", out).lower()

    return out


def remove_extra_spacing(text):
    """
    Remove double spaces (or triple, 4x, 5x etc.) and line breaks.
    Useful for printing queries and similar stuff
    """
    return re.sub(r"\s+", " ", text).strip()
