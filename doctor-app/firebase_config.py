"""
Shared Firebase project configuration.
These values are public (same as the patient website firebase-config.js).
"""

FIREBASE_API_KEY = "AIzaSyBN4vLVKqXYOsu8zIfvpxDFy2xHt9Tosus"
FIREBASE_PROJECT_ID = "anonimusq-80432"
FIREBASE_AUTH_DOMAIN = "anonimusq-80432.firebaseapp.com"

# Patient website URL (Cloudflare Workers)
PATIENT_SITE_URL = "https://anonimusq.motiml77.workers.dev"

# Fake email domain for Firebase Auth (username → email mapping)
AUTH_EMAIL_DOMAIN = "anonimusq.firebaseapp.com"


def username_to_email(username: str) -> str:
    """Convert a doctor username to a Firebase Auth email address."""
    return f"{username.lower()}@{AUTH_EMAIL_DOMAIN}"
