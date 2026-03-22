"""Fernet-based encryption for secrets stored in the database.

Uses the FERNET_KEY environment variable (or auto-generates one and writes
it to .env on first run).  All API keys stored in EngineSettings are
encrypted at rest.

Key rotation: set FERNET_KEY_OLD to the previous key to transparently
re-encrypt on next access.
"""

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from cryptography.fernet import Fernet, InvalidToken
    _HAS_FERNET = True
except ImportError:
    _HAS_FERNET = False
    InvalidToken = Exception  # fallback so references don't break
    logger.warning(
        "cryptography package not installed. Secrets will NOT be encrypted at rest. "
        "Install with: pip install cryptography"
    )

_ENGINE_DIR = Path(__file__).resolve().parent
_ENV_PATH = _ENGINE_DIR.parent / ".env"

_fernet: Optional[object] = None
_fernet_old: Optional[object] = None
_initialized: bool = False


def _get_or_create_key() -> str:
    """Return the Fernet key from env, or generate one and append it to .env."""
    key = os.getenv("FERNET_KEY", "")
    if key:
        return key

    if not _HAS_FERNET:
        return ""

    # Auto-generate a key
    new_key = Fernet.generate_key().decode()
    logger.info("Generated new FERNET_KEY (auto-appended to .env).")

    # Append to .env file
    try:
        with open(_ENV_PATH, "a", encoding="utf-8") as f:
            f.write(f"\n# --- Encryption (auto-generated, do NOT share) ---\n")
            f.write(f"FERNET_KEY={new_key}\n")
    except Exception:
        logger.exception("Failed to write FERNET_KEY to .env -- set it manually.")

    os.environ["FERNET_KEY"] = new_key
    return new_key


def _get_fernet() -> Optional[object]:
    """Return the Fernet instance (lazy-init, singleton)."""
    global _fernet, _fernet_old, _initialized

    if not _HAS_FERNET:
        return None

    if _initialized:
        return _fernet

    _initialized = True

    key = _get_or_create_key()
    if not key:
        return None

    try:
        _fernet = Fernet(key.encode() if isinstance(key, str) else key)
    except Exception:
        logger.exception("Invalid FERNET_KEY -- secrets will not be encrypted.")
        return None

    # Optional old key for rotation
    old_key = os.getenv("FERNET_KEY_OLD", "")
    if old_key:
        try:
            _fernet_old = Fernet(old_key.encode() if isinstance(old_key, str) else old_key)
        except Exception:
            logger.warning("Invalid FERNET_KEY_OLD -- key rotation will not work.")

    return _fernet


def encrypt_value(plaintext: str) -> str:
    """Encrypt a string value. Returns 'enc:...' prefixed ciphertext.

    If encryption is unavailable, returns plaintext unchanged.
    """
    f = _get_fernet()
    if f is None or not plaintext:
        return plaintext
    try:
        token = f.encrypt(plaintext.encode("utf-8"))
        return "enc:" + token.decode("utf-8")
    except Exception:
        logger.exception("Encryption failed -- storing plaintext.")
        return plaintext


def decrypt_value(stored: str) -> str:
    """Decrypt a stored value. Handles both 'enc:...' and legacy plaintext.

    If decryption fails with the current key, tries the old key (rotation).
    """
    if not stored or not stored.startswith("enc:"):
        return stored  # Legacy plaintext -- return as-is

    f = _get_fernet()
    if f is None:
        # Can't decrypt without cryptography -- strip prefix
        logger.warning("Cannot decrypt: cryptography not installed or FERNET_KEY missing.")
        return ""

    ciphertext = stored[4:].encode("utf-8")

    try:
        return f.decrypt(ciphertext).decode("utf-8")
    except InvalidToken:
        # Try old key for rotation
        if _fernet_old:
            try:
                plaintext = _fernet_old.decrypt(ciphertext).decode("utf-8")
                logger.info("Decrypted with old key -- will re-encrypt with new key on next write.")
                return plaintext
            except InvalidToken:
                pass
        logger.error("Failed to decrypt value -- invalid key or corrupted data.")
        return ""


# Convenience: list of EngineSettings keys that hold secrets
SECRET_KEYS: set = {
    "anthropic_api_key",
    "groq_api_key",
    "openrouter_api_key",
    "fred_api_key",
    "twitter_bearer_token",
    "telegram_bot_token",
    "telegram_chat_id",
}
