# Forecast Submission Tool — forecast.wbn (Web)

A Flask web application for submitting DRR (Daily Run Rate) product forecast reports
across all sales channels — **Wellbeing Nutrition**.

Mirrors the architecture of the existing `PROJECT_PBI` reporting portal (Microsoft SSO,
role-based access, same green brand palette).

---

## Project Structure

```
forecast_wbn/
├── app.py                  # Flask application & routes
├── excel_handler.py        # Excel read/write (BytesIO-compatible)
├── templates/
│   ├── login.html          # Microsoft SSO login page
│   ├── forecast.html       # Main spreadsheet UI
│   ├── access_denied.html  # 403 page
│   └── maintenance.html    # 503 maintenance page
├── static/
│   ├── style.css           # Login page styles
│   └── logo.png            # Place your logo here
├── .env.example            # Environment variable template
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Clone & install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your Azure AD credentials and user lists
```

### 3. Azure AD App Registration

1. Go to [Azure Portal](https://portal.azure.com) → Azure Active Directory → App registrations
2. Create a new registration
3. Add a redirect URI: `http://localhost:5000/getAToken` (dev) or `https://yourdomain.com/getAToken` (prod)
4. Copy **Client ID**, **Client Secret**, and **Tenant ID** into `.env`

### 4. Add users

In `.env`:
```
ADMINS=admin@yourcompany.com
FORECAST_MEMBERS=user1@yourcompany.com,user2@yourcompany.com
```

Admins have full access. `FORECAST_MEMBERS` can access the forecast tool.

### 5. Run

```bash
python app.py
```

Navigate to `http://localhost:5000`

---

## Usage Flow

| Step | Action |
|------|--------|
| 1 | Sign in with Microsoft |
| 2 | Click **Download Template** to get the blank Excel, or **Load Template** to upload a filled one |
| 3 | Edit forecast values inline in the browser grid |
| 4 | Click **Save Draft** to download a working copy (does not lock) |
| 5 | Click **Submit Forecast** — validates, locks the form, stores the submission |
| 6 | Download the final Excel via **Download Submission** |

---

## Submission Lock

Once **Submit Forecast** is clicked successfully:

- All editable cells become greyed out and non-interactive
- A yellow lock banner appears in the grid header
- The Submit and Save Draft buttons are disabled
- The session is flagged server-side — re-submission attempts are rejected with HTTP 409
- A **Download Submission** button appears to retrieve the generated Excel

To start a new submission, the user must log out and back in (new session).

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Login page |
| `GET` | `/signin` | Redirect to Microsoft login |
| `GET` | `/getAToken` | OAuth2 callback |
| `GET` | `/logout` | Clear session |
| `GET` | `/forecast` | Main forecast UI |
| `GET` | `/api/sample-data` | Returns 20 demo products as JSON |
| `GET` | `/api/download-template` | Download blank Excel template |
| `POST` | `/api/upload-template` | Upload Excel → returns parsed JSON |
| `POST` | `/api/save-draft` | Returns draft Excel as download |
| `POST` | `/api/submit` | Validate + submit + lock session |
| `GET` | `/api/download-submission` | Download submitted Excel |

---

## Channels & Periods

| Channel | Periods |
|---------|---------|
| D2C | 15D, 30D, 45D, 60D |
| M-B2B | 15D, 30D, 45D, 60D |
| M-B2C | 15D, 30D, 45D, 60D |
| Retail | 15D, 30D, 45D, 60D |
| Export | 15D, 30D, 45D, 60D |
| Amazon | 15D, 30D, 45D, 60D |
| Flipkart | 15D, 30D, 45D, 60D |

28 editable values per product (7 channels × 4 periods).

---

## Deploying to Azure / Render / Railway

### Azure App Service (recommended — matches your existing portal)

```bash
# Add startup command in Azure portal:
gunicorn app:app

# Or create a Procfile:
echo "web: gunicorn app:app" > Procfile
```

Set all `.env` variables as **Application Settings** in the Azure portal.
Update the redirect URI in your Azure AD app registration to your production domain.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Login redirect fails | Check `CLIENT_ID`, `CLIENT_SECRET`, `TENANT_ID` in `.env` and redirect URI in Azure portal |
| Access denied after login | Add user email to `FORECAST_MEMBERS` or `ADMINS` in `.env` |
| Excel upload fails | Ensure the file uses the template column names — download the template first |
| Session lock not working | Make sure `SECRET_KEY` is set and consistent across restarts |
| Logo not showing | Place `logo.png` in the `static/` folder |

---

*Wellbeing Nutrition — forecast.wbn*
