# forecast.wbn v6 — Wellbeing Nutrition

## What's New in v6

### 🔒 2-Month Rolling Lock
- **Current month + next month are always locked** after submission.
- In April → April & May locked → only June available for refill.
- In May → May & June locked → only July available (if in same quarter).
- Clear lock indicators in banners, submit modal, and success screen.

### 🧮 Floating Calculator
- Always-accessible calculator FAB button (bottom-right corner).
- Draggable — move it anywhere on screen.
- Memory functions: MC, MR, M+, M−.
- Keyboard support: numbers, operators, Enter, Backspace, Escape.
- Minimize/restore without closing.

### 🗄️ PostgreSQL Database Layer
- Set `DATABASE_URL` in `.env` to enable full persistence.
- Leave blank to use fast in-memory mode (dev/demo).
- Auto-creates all tables on first run.
- See `DB_SETUP.md` for full setup guide (local / Render / Supabase / Railway).

## Quick Start

```bash
pip install -r requirements.txt
# Edit .env with your Azure AD credentials
python app.py
```

## Lock Logic Reference

| Current Month | Locked (cannot refill) | Available for refill |
|---------------|----------------------|---------------------|
| April (Q1) | April, May | **June** |
| May (Q1) | May, June | *(none — Q1 exhausted)* |
| June (Q1) | June | *(none)* |
| July (Q2) | July, August | **September** |
| August (Q2) | August, September | *(none)* |

## File Structure

```
forecast_v6/
├── app.py              # Main Flask app (lock logic here)
├── db.py               # PostgreSQL layer (auto-loaded if DATABASE_URL set)
├── excel_handler.py    # Excel parsing helpers
├── requirements.txt    # Python dependencies
├── .env                # Secrets & config (never commit)
├── DB_SETUP.md         # Full database setup guide
├── static/
│   ├── logo.png
│   └── style.css
└── templates/
    ├── forecast.html   # Main forecast UI (floating calc, lock banners)
    ├── admin.html
    ├── insights.html
    ├── login.html
    ├── maintenance.html
    └── access_denied.html
```
