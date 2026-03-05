"""Password-derived encryption utilities for AnonimousQ.

All sensitive patient data (names, phones, notes, emergency contacts, broker names)
is encrypted using a Fernet key derived from the doctor's password via PBKDF2.

Key principle: whoever doesn't have the password cannot read anything in Firebase.
"""

import os
import hashlib
import base64

from cryptography.fernet import Fernet, InvalidToken

# ── Constants ────────────────────────────────────────────────────────────────
PBKDF2_ITERATIONS = 600_000
SALT_LENGTH = 16  # bytes
KEY_CHECK_PLAINTEXT = "ANONIMUSQ_KEY_CHECK"

# ── Module-level cache (in-memory only, never persisted) ─────────────────────
_cached_fernet = None


# ── Salt generation ──────────────────────────────────────────────────────────

def generate_salt() -> bytes:
    """Generate a cryptographically random salt (16 bytes)."""
    return os.urandom(SALT_LENGTH)


def salt_to_b64(salt: bytes) -> str:
    """Encode salt bytes to base64 string (for storage)."""
    return base64.b64encode(salt).decode("ascii")


def b64_to_salt(b64: str) -> bytes:
    """Decode base64 string back to salt bytes."""
    return base64.b64decode(b64)


# ── Key derivation ───────────────────────────────────────────────────────────

def derive_fernet_key(password: str, salt: bytes) -> bytes:
    """Derive a 32-byte Fernet key from password + salt using PBKDF2-HMAC-SHA256.

    Returns the raw Fernet key (base64-encoded 32 bytes, as Fernet expects).
    """
    raw = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
        dklen=32,
    )
    return base64.urlsafe_b64encode(raw)


def create_fernet(password: str, salt: bytes) -> Fernet:
    """Create a Fernet instance from password + salt."""
    key = derive_fernet_key(password, salt)
    return Fernet(key)


# ── Verification token ──────────────────────────────────────────────────────

def make_verification_token(fernet: Fernet) -> str:
    """Encrypt a known plaintext to produce a verification token.
    Used to verify that a password produces the correct key on another device.
    """
    return fernet.encrypt(KEY_CHECK_PLAINTEXT.encode("utf-8")).decode("utf-8")


def verify_key(fernet: Fernet, token: str) -> bool:
    """Check if the Fernet key can decrypt the verification token."""
    try:
        plaintext = fernet.decrypt(token.encode("utf-8")).decode("utf-8")
        return plaintext == KEY_CHECK_PLAINTEXT
    except (InvalidToken, Exception):
        return False


# ── Cached Fernet management ────────────────────────────────────────────────

def set_cached_fernet(fernet: Fernet):
    """Store the session Fernet in memory. Called after login."""
    global _cached_fernet
    _cached_fernet = fernet


def get_cached_fernet() -> Fernet:
    """Get the cached Fernet. Returns None if not set (not logged in)."""
    return _cached_fernet


def clear_cached_fernet():
    """Clear the cached Fernet from memory. Called on logout."""
    global _cached_fernet
    _cached_fernet = None


def is_ready() -> bool:
    """Check if encryption is ready (Fernet is cached in memory)."""
    return _cached_fernet is not None


# ── Encrypt / Decrypt helpers ────────────────────────────────────────────────

def encrypt(plaintext: str) -> str:
    """Encrypt plaintext using the cached Fernet key.
    Raises RuntimeError if no key is cached.
    """
    if _cached_fernet is None:
        raise RuntimeError("Encryption key not available — user not logged in")
    return _cached_fernet.encrypt(plaintext.encode("utf-8")).decode("utf-8")


def decrypt(ciphertext: str) -> str:
    """Decrypt ciphertext using the cached Fernet key.
    Returns '[שגיאת פענוח]' on failure.
    """
    if _cached_fernet is None:
        return "[שגיאת פענוח]"
    try:
        return _cached_fernet.decrypt(ciphertext.encode("utf-8")).decode("utf-8")
    except (InvalidToken, Exception):
        return "[שגיאת פענוח]"


def decrypt_with(fernet: Fernet, ciphertext: str) -> str:
    """Decrypt ciphertext with a specific Fernet instance (used during migration)."""
    try:
        return fernet.decrypt(ciphertext.encode("utf-8")).decode("utf-8")
    except (InvalidToken, Exception):
        return "[שגיאת פענוח]"


def encrypt_with(fernet: Fernet, plaintext: str) -> str:
    """Encrypt plaintext with a specific Fernet instance (used during migration)."""
    return fernet.encrypt(plaintext.encode("utf-8")).decode("utf-8")
