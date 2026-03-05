# AnonimousQ - מערכת תורים אנונימיים

## מבנה הפרויקט
```
anonimusQ/
├── electron-app/      # תוכנת המטפל (Electron + React)
├── patient-website/   # אתר המטופל (HTML סטטי)
└── firebase/          # כללי Firebase Security Rules
```

---

## שלב 1 - הגדרת Firebase (חד פעמי)

1. לך ל-[Firebase Console](https://console.firebase.google.com/)
2. צור פרויקט חדש
3. הפעל **Firestore Database** (Production mode)
4. הפעל **Authentication** → Sign-in method → **Anonymous** (Enable)
5. העתק את ה-Firestore Rules מ-`firebase/firestore.rules`
6. הוסף **Web App** לפרויקט וקבל את ה-config

---

## שלב 2 - הגדרת אתר המטופל

1. פתח `patient-website/firebase-config.js`
2. החלף את הערכים עם ה-config שקיבלת מ-Firebase
3. פרסם ב-Firebase Hosting:
   ```
   npm install -g firebase-tools
   firebase login
   firebase init hosting   (בחר את תיקיית patient-website)
   firebase deploy
   ```

---

## שלב 3 - הרצת תוכנת המטפל

```bash
cd electron-app
npm install
npm run dev
```

### בגרסת Production:
```bash
npm run dist
```
קובץ ה-installer ימצא ב-`release/`

---

## שלב 4 - שימוש ראשוני

1. פתח את האפליקציה
2. הגדר סיסמא (פעם ראשונה בלבד)
3. עבור להגדרות → חבר Firebase עם ה-config שלך
4. הגדר ימי ושעות עבודה ולחץ "שמור הגדרות"
5. עבור למטופלים → הוסף מטופל → שתף את קוד הזיהוי עם המטופל

---

## זרימת עבודה

```
מטפל מוסיף מטופל → מקבל UUID → מוסר ל-מטופל
מטופל נכנס לאתר עם UUID → קובע תור → נשמר ב-Firebase
אפליקציית מטפל מסנכרנת → ממפה UUID → שם → מציג לוח שנה מלא
```

---

## אבטחה

- **שמות המטופלים** - שמורים אך ורק לוקלית ב-SQLite במחשב המטפל
- **Firebase** - מכיל UUID + מועד בלבד, ללא שמות
- **כל מטופל** - יכול לגשת רק לתורים שלו (Firebase Rules)
- **סיסמת המטפל** - מאוחסנת כ-bcrypt hash בלבד
