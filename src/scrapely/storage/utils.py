# SPDX-License-Identifier: Apache-2.0
# Derived from apify-sdk-python (src/apify/storage_clients/_apify/_utils.py),
# Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
# Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
"""Utility functions for Scrapely storage clients."""

import base64
import hashlib
import re


def unique_key_to_request_id(unique_key: str, request_id_length: int = 15) -> str:
    """Generate a deterministic request ID based on a unique key.

    This function creates a URL-safe, truncated request ID from a unique key using
    SHA-256 hashing and base64 encoding. The same unique key will always produce
    the same request ID, allowing for deterministic lookups without storing a mapping.

    Args:
        unique_key: The unique key to convert into a request ID.
        request_id_length: The length of the resulting request ID (default 15).

    Returns:
        A URL-safe, truncated request ID based on the unique key.

    Example:
        >>> unique_key_to_request_id("https://example.com")
        'aBcDeFgHiJkLmNo'
        >>> unique_key_to_request_id("https://example.com")  # Same input
        'aBcDeFgHiJkLmNo'  # Same output
    """
    # Encode the unique key and compute its SHA-256 hash
    hashed_key = hashlib.sha256(unique_key.encode('utf-8')).digest()

    # Encode the hash in base64 and decode it to get a string
    base64_encoded = base64.b64encode(hashed_key).decode('utf-8')

    # Remove characters that are not URL-safe ('+', '/', or '=')
    url_safe_key = re.sub(r'(\+|/|=)', '', base64_encoded)

    # Truncate the key to the desired length
    return url_safe_key[:request_id_length]