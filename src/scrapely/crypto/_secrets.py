"""Input secret decryption for the Scrapely Python SDK.

Port of the Node SDK's ``src/crypto/secrets.ts``. The platform encrypts secret
Actor input values with RSA-OAEP (SHA-256) and delivers the RSA private key to
the container as a passphrase-encrypted file (AES-256-GCM with a PBKDF2-derived
key). This module decrypts the private key, decrypts each secret field, and
merges the plaintext values back into the Actor input.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger(__name__)

# ─── Constants (must match the Node SDK's crypto/secrets.ts) ──────────────────

_IV_LENGTH = 12
_AUTH_TAG_LENGTH = 16
_PBKDF2_ITERATIONS = 100_000
_PBKDF2_HASH = 'sha256'
_SALT_LENGTH = 32
_KEY_LENGTH = 32

_OAEP_PADDING = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)


def decrypt_private_key_file(private_key_file_path: str, passphrase: str) -> str:
    """Read the private key file from disk and decrypt it with the run passphrase.

    The file layout is ``salt(32) | iv(12) | authTag(16) | ciphertext``, where the
    AES-256-GCM key is derived from the passphrase with PBKDF2-HMAC-SHA256.

    Args:
        private_key_file_path: Path to the encrypted private key file
            (e.g. ``/run/secrets/private_key.pem``).
        passphrase: The run passphrase from ``APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE``.

    Returns:
        The decrypted private key as a PEM string.
    """
    encrypted_data = Path(private_key_file_path).read_bytes()

    salt = encrypted_data[:_SALT_LENGTH]
    iv = encrypted_data[_SALT_LENGTH:_SALT_LENGTH + _IV_LENGTH]
    auth_tag = encrypted_data[_SALT_LENGTH + _IV_LENGTH:_SALT_LENGTH + _IV_LENGTH + _AUTH_TAG_LENGTH]
    encrypted = encrypted_data[_SALT_LENGTH + _IV_LENGTH + _AUTH_TAG_LENGTH:]

    key = hashlib.pbkdf2_hmac(
        _PBKDF2_HASH,
        passphrase.encode('utf-8'),
        salt,
        _PBKDF2_ITERATIONS,
        dklen=_KEY_LENGTH,
    )

    # The ``cryptography`` AESGCM helper expects ciphertext || tag (the file
    # stores tag before the ciphertext, like Node's setAuthTag() flow).
    decrypted = AESGCM(key).decrypt(iv, encrypted + auth_tag, None)

    return decrypted.decode('utf-8')


def decrypt_secret_value(encrypted_base64: str, private_key_pem: str) -> Any:
    """Decrypt a single secret value encrypted with RSA-OAEP (SHA-256).

    Args:
        encrypted_base64: Base64-encoded encrypted data.
        private_key_pem: The decrypted private key PEM.

    Returns:
        The decrypted value; JSON-shaped plaintext is parsed into Python objects,
        anything else is returned as a plain string.
    """
    encrypted = base64.b64decode(encrypted_base64)

    private_key = serialization.load_pem_private_key(private_key_pem.encode('utf-8'), password=None)
    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise ValueError('The decrypted private key is not an RSA key')
    decrypted_data = private_key.decrypt(encrypted, _OAEP_PADDING)

    plaintext = decrypted_data.decode('utf-8')

    # Try to parse as JSON (for objects, arrays, numbers, booleans)
    try:
        return json.loads(plaintext)
    except json.JSONDecodeError:
        return plaintext


def decrypt_all_secrets(
    encrypted_secrets: Dict[str, str],
    private_key_pem: str,
) -> Dict[str, Any]:
    """Decrypt all secret values from the encrypted input payload.

    Args:
        encrypted_secrets: Map of field name to base64 encrypted value.
        private_key_pem: The decrypted private key PEM.

    Returns:
        Map of field name to decrypted value; fields that fail to decrypt are
        skipped with a warning (matching the Node SDK behavior).
    """
    decrypted: Dict[str, Any] = {}

    for field_name, encrypted_base64 in encrypted_secrets.items():
        try:
            decrypted[field_name] = decrypt_secret_value(encrypted_base64, private_key_pem)
        except Exception as err:  # noqa: BLE001 - a bad field must not kill the run
            logger.warning('Failed to decrypt secret field "%s": %s', field_name, err)

    return decrypted


def merge_decrypted_input(
    input_data: Dict[str, Any],
    decrypted_secrets: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge decrypted secret values into the original input object.

    Supports nested paths like ``credentials.apiKey`` or ``database.password``.

    Args:
        input_data: The original input object (with secrets stripped/masked).
        decrypted_secrets: Map of field name to decrypted value.

    Returns:
        A new input object with secrets populated.
    """
    merged = dict(input_data)

    for field_name, value in decrypted_secrets.items():
        if '.' in field_name:
            # Handle nested paths
            parts = field_name.split('.')
            current = merged
            for part in parts[:-1]:
                if not isinstance(current.get(part), dict):
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        else:
            merged[field_name] = value

    return merged


def decrypt_secret_inputs(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Full decryption flow: read the private key file, decrypt it with the passphrase,
    then decrypt all secrets and merge them into the input.

    This is called from ``Actor.get_input()`` when
    ``APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE`` is set.

    Args:
        input_data: Current Actor input object.

    Returns:
        The input object with decrypted secret values merged in; on failure the
        input is returned as-is so the Actor can still run.
    """
    private_key_file = os.environ.get('APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE')
    passphrase = os.environ.get('APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE')

    if not private_key_file or not passphrase:
        return input_data  # No secrets to decrypt

    logger.info('Decrypting secret inputs...')

    try:
        # 1. Read and decrypt the private key file
        private_key_pem = decrypt_private_key_file(private_key_file, passphrase)
        logger.info('Private key decrypted successfully')

        # 2. Get the encrypted secrets from the input
        # The encrypted secrets are stored in a special __encrypted_secrets property
        encrypted_secrets = input_data.get('__encrypted_secrets')

        if not encrypted_secrets or not isinstance(encrypted_secrets, dict) or not encrypted_secrets:
            logger.info('No encrypted secrets found in input')
            return input_data

        # 3. Decrypt all secret values
        decrypted_secrets = decrypt_all_secrets(encrypted_secrets, private_key_pem)
        logger.info('Decrypted %d secret field(s)', len(decrypted_secrets))

        # 4. Merge decrypted values into input and remove the __encrypted_secrets key
        merged = merge_decrypted_input(input_data, decrypted_secrets)
        merged.pop('__encrypted_secrets', None)
        merged.pop('__encrypted_secret_fields', None)

        return merged
    except Exception as err:  # noqa: BLE001 - secrets won't be available but actor can still run
        logger.error('Failed to decrypt secret inputs: %s', err)
        return input_data
