"""
Firebase Authentication via REST API.
Handles doctor registration, login, and password management
using Firebase Auth email/password provider.
"""

import requests

from firebase_config import FIREBASE_API_KEY, username_to_email

_AUTH_BASE = "https://identitytoolkit.googleapis.com/v1"
_TOKEN_BASE = "https://securetoken.googleapis.com/v1"


def register(username: str, password: str) -> dict:
    """Register a new doctor in Firebase Auth.
    Returns {"ok": True, "uid": "..."} or {"ok": False, "error": "..."}
    """
    email = username_to_email(username)
    try:
        resp = requests.post(
            f"{_AUTH_BASE}/accounts:signUp",
            params={"key": FIREBASE_API_KEY},
            json={
                "email": email,
                "password": password,
                "returnSecureToken": True,
            },
            timeout=15,
        )
        data = resp.json()

        if resp.ok:
            return {"ok": True, "uid": data.get("localId", "")}

        error_code = (
            data.get("error", {}).get("message", "UNKNOWN_ERROR")
        )
        if "EMAIL_EXISTS" in error_code:
            return {"ok": False, "error": "שם המשתמש כבר רשום במערכת — עבור ללשונית \"יש לי חשבון\" והתחבר עם הסיסמא שהגדרת"}
        if "WEAK_PASSWORD" in error_code:
            return {"ok": False, "error": "הסיסמא חלשה מדי (מינימום 6 תווים)"}
        return {"ok": False, "error": f"שגיאת הרשמה: {error_code}"}

    except requests.ConnectionError:
        return {"ok": False, "error": "אין חיבור לאינטרנט – נדרש חיבור להרשמה"}
    except Exception as e:
        return {"ok": False, "error": f"שגיאת הרשמה: {e}"}


def login(username: str, password: str) -> dict:
    """Authenticate doctor against Firebase Auth.
    Returns {"ok": True, "uid": "...", "idToken": "..."} or {"ok": False, ...}
    """
    email = username_to_email(username)
    try:
        resp = requests.post(
            f"{_AUTH_BASE}/accounts:signInWithPassword",
            params={"key": FIREBASE_API_KEY},
            json={
                "email": email,
                "password": password,
                "returnSecureToken": True,
            },
            timeout=15,
        )
        data = resp.json()

        if resp.ok:
            return {
                "ok": True,
                "uid": data.get("localId", ""),
                "idToken": data.get("idToken", ""),
                "refreshToken": data.get("refreshToken", ""),
            }

        error_code = (
            data.get("error", {}).get("message", "UNKNOWN_ERROR")
        )
        if "EMAIL_NOT_FOUND" in error_code:
            return {"ok": False, "error": "שם משתמש לא קיים"}
        if "INVALID_PASSWORD" in error_code or "INVALID_LOGIN_CREDENTIALS" in error_code:
            return {"ok": False, "error": "סיסמא שגויה"}
        if "USER_DISABLED" in error_code:
            return {"ok": False, "error": "החשבון חסום"}
        return {"ok": False, "error": f"שגיאת כניסה: {error_code}"}

    except requests.ConnectionError:
        return {"ok": False, "error": "offline"}
    except Exception as e:
        return {"ok": False, "error": f"שגיאת כניסה: {e}"}


def change_password(id_token: str, new_password: str) -> dict:
    """Change the password for the currently authenticated user.
    Requires a valid id_token from a recent login.
    Returns {"ok": True} or {"ok": False, "error": "..."}
    """
    try:
        resp = requests.post(
            f"{_AUTH_BASE}/accounts:update",
            params={"key": FIREBASE_API_KEY},
            json={
                "idToken": id_token,
                "password": new_password,
                "returnSecureToken": True,
            },
            timeout=15,
        )
        data = resp.json()

        if resp.ok:
            return {"ok": True}

        error_code = data.get("error", {}).get("message", "UNKNOWN_ERROR")
        return {"ok": False, "error": f"שגיאת שינוי סיסמא: {error_code}"}

    except requests.ConnectionError:
        return {"ok": False, "error": "אין חיבור לאינטרנט"}
    except Exception as e:
        return {"ok": False, "error": f"שגיאה: {e}"}


def username_exists(username: str) -> bool:
    """Check if a username is already registered in Firebase Auth.
    Uses Firebase Admin SDK (get_user_by_email) for reliable existence check.
    """
    email = username_to_email(username)
    try:
        from firebase_admin import auth
        auth.get_user_by_email(email)
        return True
    except Exception:
        return False
