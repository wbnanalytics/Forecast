"""
app.py — forecast.wbn | Wellbeing Nutrition
Quarter-wise, channel-wise DRR forecast tool with filters and DRR prefill.
"""
import os, io, smtplib, ssl, threading, datetime, json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from functools import wraps

from flask import (Flask, render_template, redirect, url_for,
                   session, request, jsonify, send_file)
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix
import msal

from excel_handler import (
    CHANNELS, get_sample_products, save_submission_log,
)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "forecast-wbn-dev-secret")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

MAINTENANCE_MODE   = os.getenv("MAINTENANCE_MODE", "false").lower() == "true"
MAINTENANCE_BYPASS = [e.strip().lower() for e in os.getenv("MAINTENANCE_BYPASS_EMAILS","").split(",") if e.strip()]

CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID     = os.getenv("TENANT_ID")
AUTHORITY     = "https://login.microsoftonline.com/" + (TENANT_ID or "")
REDIRECT_PATH = "/getAToken"
SCOPE         = ["User.Read"]

def get_redirect_uri():
    scheme = "http" if request.host.startswith(("localhost","127.0.0.1")) else "https"
    return url_for("authorized", _external=True, _scheme=scheme)

def _msal_app():
    return msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)

def _auth_url():
    return _msal_app().get_authorization_request_url(SCOPE, redirect_uri=get_redirect_uri())

ADMINS           = [e.strip().lower() for e in os.getenv("ADMINS","").split(",") if e.strip()]
FORECAST_MEMBERS = [e.strip().lower() for e in os.getenv("FORECAST_MEMBERS","").split(",") if e.strip()]

def _build_channel_map():
    raw = os.getenv("CHANNEL_MAP", "")
    m = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" in part:
            email, ch = part.rsplit(":", 1)
            m[email.strip().lower()] = ch.strip()
    return m

CHANNEL_MAP = _build_channel_map()

NOTIFY_EMAILS = [e.strip() for e in os.getenv("NOTIFY_EMAILS",
    "nivas@wellbeingnutrition.com,rushikesh.pawar@wellbeingnutrition.com").split(",") if e.strip()]
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

QUARTERS = {
    "Q1": {"label":"Q1 (Apr-Jun)", "months":["April","May","June"],           "short":["Apr","May","Jun"]},
    "Q2": {"label":"Q2 (Jul-Sep)", "months":["July","August","September"],    "short":["Jul","Aug","Sep"]},
    "Q3": {"label":"Q3 (Oct-Dec)", "months":["October","November","December"],"short":["Oct","Nov","Dec"]},
    "Q4": {"label":"Q4 (Jan-Mar)", "months":["January","February","March"],   "short":["Jan","Feb","Mar"]},
}

DRR_LABELS = ["7 Days Overall DRR","15 Days Overall DRR","30 Days Overall DRR",
               "45 Days Overall DRR","60 Days Overall DRR"]
DRR_SHORT  = ["7D","15D","30D","45D","60D"]

BASE_COLS_DRR = ["Category","Sub-Category","Product Type",
                 "Product Name","SKU","Live/Not Live","Sub Product Type"]

_lock        = threading.Lock()
_quarters    = {}
_submissions = {}
_admin_log   = []

def _sub_default():
    return dict(submitted=False, submitted_at="", data=None, user_name="",
                revision=0, refill_requested=False, refill_reason="",
                refill_approved=False, excel_bytes=None, file="")

def _q_get(qkey):
    with _lock: return dict(_quarters.get(qkey, {}))

def _q_set(qkey, data):
    with _lock: _quarters[qkey] = data

def _q_revoke(qkey):
    with _lock:
        _quarters.pop(qkey, None)
        _submissions.pop(qkey, None)

def _sub_get(qkey, channel):
    with _lock: return dict(_submissions.get(qkey,{}).get(channel, _sub_default()))

def _sub_set(qkey, channel, updates):
    with _lock:
        if qkey not in _submissions: _submissions[qkey] = {}
        if channel not in _submissions[qkey]: _submissions[qkey][channel] = _sub_default()
        _submissions[qkey][channel].update(updates)

def _sub_reset(qkey, channel):
    with _lock:
        if qkey in _submissions and channel in _submissions[qkey]:
            _submissions[qkey][channel] = _sub_default()

def _all_subs(qkey):
    with _lock: return {ch: dict(s) for ch, s in _submissions.get(qkey,{}).items()}

def _log(action, user, detail=""):
    with _lock:
        _admin_log.append(dict(
            timestamp=datetime.datetime.now().strftime("%d %b %Y, %H:%M"),
            action=action, user=user, detail=detail))

def _send_email(subject, body, attach_name=None, attach_bytes=None):
    if not SMTP_USER or not SMTP_PASS:
        app.logger.warning("SMTP not configured — email skipped.")
        return
    def _work():
        try:
            msg = MIMEMultipart()
            msg["From"] = SMTP_USER; msg["To"] = ", ".join(NOTIFY_EMAILS)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))
            if attach_name and attach_bytes:
                part = MIMEBase("application","octet-stream")
                part.set_payload(attach_bytes); encoders.encode_base64(part)
                part.add_header("Content-Disposition",'attachment; filename="'+attach_name+'"')
                msg.attach(part)
            ctx = ssl.create_default_context()
            with smtplib.SMTP("smtp.office365.com", 587) as srv:
                srv.ehlo(); srv.starttls(context=ctx); srv.ehlo()
                srv.login(SMTP_USER, SMTP_PASS)
                srv.sendmail(SMTP_USER, NOTIFY_EMAILS, msg.as_string())
        except Exception as e: app.logger.error("Email failed: "+str(e))
    threading.Thread(target=_work, daemon=True).start()

def _require_login(f):
    @wraps(f)
    def d(*a, **kw):
        if not session.get("user"): return redirect(url_for("login"))
        email = session["user"].get("preferred_username","").lower().strip()
        if MAINTENANCE_MODE and email not in MAINTENANCE_BYPASS:
            return render_template("maintenance.html"), 503
        if email not in ADMINS and email not in FORECAST_MEMBERS:
            return render_template("access_denied.html"), 403
        return f(*a, **kw)
    return d

def _require_admin(f):
    @wraps(f)
    def d(*a, **kw):
        if not session.get("user"): return redirect(url_for("login"))
        if session["user"].get("preferred_username","").lower().strip() not in ADMINS:
            return render_template("access_denied.html"), 403
        return f(*a, **kw)
    return d

def _email():  return session["user"].get("preferred_username","").lower().strip()
def _name():   return session["user"].get("name", _email())
def _is_admin(): return _email() in ADMINS
def _user_channel(email): return CHANNEL_MAP.get(email.lower())

# ── Auth ────────────────────────────────────────────────────────────────────────
@app.route("/")
def login():
    if session.get("user"): return redirect(url_for("forecast"))
    return render_template("login.html")

@app.route("/signin")
def signin(): return redirect(_auth_url())

@app.route(REDIRECT_PATH)
def authorized():
    result = _msal_app().acquire_token_by_authorization_code(
        request.args.get("code",""), scopes=SCOPE, redirect_uri=get_redirect_uri())
    if "error" in result:
        return "Login error: "+str(result.get("error_description")), 400
    session["user"] = result.get("id_token_claims")
    email = session["user"].get("preferred_username","").lower().strip()
    if email not in ADMINS and email not in FORECAST_MEMBERS:
        return render_template("access_denied.html"), 403
    return redirect(url_for("forecast"))

@app.route("/logout")
def logout(): session.clear(); return redirect(url_for("login"))

# ── Forecast page ──────────────────────────────────────────────────────────────
@app.route("/forecast")
@_require_login
def forecast():
    email  = _email(); name = _name()
    is_adm = _is_admin()
    ch     = _user_channel(email) if not is_adm else None
    q_status = {}
    for qkey in QUARTERS:
        q = _q_get(qkey)
        if not q.get("initiated"):
            q_status[qkey] = {"initiated": False}
        else:
            subs = _all_subs(qkey)
            q_status[qkey] = {
                "initiated": True,
                "initiated_at": q.get("initiated_at",""),
                "sku_count": len(q.get("drr_data") or []),
                "channels": {c: {
                    "submitted": subs.get(c,{}).get("submitted",False),
                    "submitted_at": subs.get(c,{}).get("submitted_at",""),
                } for c in CHANNELS},
            }
    return render_template("forecast.html",
        user_name=name, user_email=email,
        is_admin=is_adm, user_channel=ch,
        channels=CHANNELS, quarters=QUARTERS,
        drr_labels=DRR_LABELS, drr_short=DRR_SHORT,
        q_status=q_status,
        today=datetime.date.today().strftime("%A, %d %B %Y"),
    )

# ── API: Load DRR data for a quarter/channel ───────────────────────────────────
@app.route("/api/load-drr/<qkey>")
@_require_login
def api_load_drr(qkey):
    if qkey not in QUARTERS: return jsonify({"error":"Invalid quarter"}), 400
    q = _q_get(qkey)
    if not q.get("initiated"): return jsonify({"error":"Quarter not initiated"}), 404
    email  = _email(); is_adm = _is_admin()
    ch     = request.args.get("channel") if is_adm else _user_channel(email)
    if not ch: return jsonify({"error":"No channel assigned"}), 400
    drr_data = q.get("drr_data") or []
    sub      = _sub_get(qkey, ch)
    saved    = sub.get("data") or {}

    rows = []
    for row in drr_data:
        r = dict(row)
        # Strip internal _drr blob before sending (we send channel-specific drr separately)
        drr_for_ch = r.pop("_drr", {}).get(ch, {})
        row_saved  = saved.get(r.get("_row_id",""), {})
        for m in QUARTERS[qkey]["months"]:
            r[m] = row_saved.get(m, "")
        # Attach DRR values for this channel as _ref
        r["_ref"] = {dl: round(drr_for_ch.get(dl, 0), 2) for dl in DRR_LABELS}
        rows.append(r)

    # Build filter options from this data
    cats    = sorted(set(r.get("Category","") for r in rows if r.get("Category")))
    subcats = sorted(set(r.get("Sub-Category","") for r in rows if r.get("Sub-Category")))
    ptypes  = sorted(set(r.get("Product Type","") for r in rows if r.get("Product Type")))

    return jsonify({
        "rows": rows,
        "months": QUARTERS[qkey]["months"],
        "drr_labels": DRR_LABELS,
        "drr_short": DRR_SHORT,
        "channel": ch,
        "submitted": sub.get("submitted", False),
        "submitted_at": sub.get("submitted_at",""),
        "revision": sub.get("revision", 0),
        "refill_requested": sub.get("refill_requested", False),
        "refill_approved": sub.get("refill_approved", False),
        "filter_options": {
            "categories": cats,
            "sub_categories": subcats,
            "product_types": ptypes,
        }
    })

# ── API: Sample data ───────────────────────────────────────────────────────────
@app.route("/api/sample-data/<qkey>")
@_require_login
def api_sample_data(qkey):
    if qkey not in QUARTERS: return jsonify({"error":"Invalid quarter"}), 400
    import random; random.seed(42)
    q = _q_get(qkey)
    products = q.get("drr_data") or get_sample_products()
    months   = QUARTERS[qkey]["months"]
    rows = []
    for i, p in enumerate(products):
        row = dict(p)
        row.pop("_drr", None)
        row.setdefault("_row_id", "r"+str(i))
        for m in months: row[m] = ""
        row["_ref"] = {dl: 0 for dl in DRR_LABELS}
        rows.append(row)
    cats    = sorted(set(r.get("Category","") for r in rows if r.get("Category")))
    subcats = sorted(set(r.get("Sub-Category","") for r in rows if r.get("Sub-Category")))
    ptypes  = sorted(set(r.get("Product Type","") for r in rows if r.get("Product Type")))
    return jsonify({
        "rows": rows, "months": months,
        "filter_options": {"categories": cats, "sub_categories": subcats, "product_types": ptypes}
    })

# ── API: Save draft ────────────────────────────────────────────────────────────
@app.route("/api/save-draft/<qkey>", methods=["POST"])
@_require_login
def api_save_draft(qkey):
    if qkey not in QUARTERS: return jsonify({"error":"Invalid quarter"}), 400
    email  = _email(); is_adm = _is_admin()
    ch     = request.json.get("channel") if is_adm else _user_channel(email)
    if not ch: return jsonify({"error":"No channel"}), 400
    sub = _sub_get(qkey, ch)
    if sub["submitted"] and not sub["refill_approved"]:
        return jsonify({"error":"Already submitted"}), 409
    rows = request.json.get("rows",[])
    data = {row.get("_row_id",""): {m: row.get(m,"") for m in QUARTERS[qkey]["months"]} for row in rows}
    _sub_set(qkey, ch, {"data": data, "user_name": _name()})
    return jsonify({"status":"saved"})

# ── API: Submit ────────────────────────────────────────────────────────────────
@app.route("/api/submit/<qkey>", methods=["POST"])
@_require_login
def api_submit(qkey):
    if qkey not in QUARTERS: return jsonify({"error":"Invalid quarter"}), 400
    email  = _email(); name = _name(); is_adm = _is_admin()
    ch     = request.json.get("channel") if is_adm else _user_channel(email)
    if not ch: return jsonify({"error":"No channel"}), 400
    sub = _sub_get(qkey, ch)
    if sub["submitted"] and not sub["refill_approved"]:
        return jsonify({"error":"Already submitted","submitted_at":sub["submitted_at"]}), 409
    rows = request.json.get("rows",[])
    if not rows: return jsonify({"error":"No data"}), 400
    months = QUARTERS[qkey]["months"]
    errors = []
    for row in rows:
        sku = row.get("SKU") or row.get("Product Name","?")
        for m in months:
            v = row.get(m,"")
            if v == "" or v is None: errors.append("["+sku+"] '"+m+"' is empty.")
            else:
                try: float(str(v).replace(",",""))
                except ValueError: errors.append("["+sku+"] '"+m+"' invalid value.")
    if errors: return jsonify({"error":"Validation failed","details":errors[:10]}), 422
    data = {row.get("_row_id",""): {m: row.get(m,"") for m in months} for row in rows}
    buf  = io.BytesIO(); _save_channel_excel(rows, name, ch, qkey, buf); buf.seek(0)
    eb   = buf.read()
    at   = datetime.datetime.now().strftime("%d %b %Y, %H:%M")
    rev  = sub["revision"] + 1
    fn   = ("Forecast_"+qkey+"_"+ch+"_"+datetime.date.today().strftime("%Y_%m_%d")
            +"_"+name.replace(" ","_")+("_r"+str(rev) if rev>1 else "")+".xlsx")
    _sub_set(qkey, ch, dict(submitted=True, submitted_at=at, data=data, user_name=name,
                             revision=rev, refill_requested=False, refill_reason="",
                             refill_approved=False, excel_bytes=eb, file=fn))
    try: save_submission_log(name+" ("+ch+")", fn, len(rows), ".")
    except: pass
    _log("Submission", name, qkey+"/"+ch+" Rev "+str(rev)+" — "+str(len(rows))+" rows")
    _send_email(
        subject="Forecast Submitted — "+name+"/"+ch+"/"+qkey+" — "+datetime.date.today().strftime("%d %b %Y"),
        body="User: "+name+" ("+email+")\nQuarter: "+qkey+"\nChannel: "+ch+"\nDate: "+at+"\nRows: "+str(len(rows))+"\nRev: "+str(rev)+"\n\n— forecast.wbn",
        attach_name=fn, attach_bytes=eb)
    return jsonify({"status":"success","submitted_at":at,"filename":fn,"revision":rev})

# ── API: Download submission ───────────────────────────────────────────────────
@app.route("/api/download-submission/<qkey>/<channel>")
@_require_login
def api_download_submission(qkey, channel):
    email = _email(); is_adm = _is_admin()
    if not is_adm and _user_channel(email) != channel:
        return jsonify({"error":"Access denied"}), 403
    sub = _sub_get(qkey, channel)
    if not sub.get("submitted"): return jsonify({"error":"No submission"}), 404
    eb = sub.get("excel_bytes")
    if not eb: return jsonify({"error":"File unavailable"}), 404
    buf = io.BytesIO(eb); buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=sub.get("file","submission.xlsx"),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── API: Refill request ────────────────────────────────────────────────────────
@app.route("/api/request-refill/<qkey>/<channel>", methods=["POST"])
@_require_login
def api_request_refill(qkey, channel):
    email = _email(); name = _name(); is_adm = _is_admin()
    if not is_adm and _user_channel(email) != channel:
        return jsonify({"error":"Access denied"}), 403
    sub = _sub_get(qkey, channel)
    if not sub.get("submitted"): return jsonify({"error":"Nothing submitted"}), 400
    if sub.get("refill_requested") and not sub.get("refill_approved"):
        return jsonify({"error":"Already requested"}), 409
    reason = (request.json or {}).get("reason","").strip()
    if not reason: return jsonify({"error":"Please provide a reason"}), 400
    _sub_set(qkey, channel, {"refill_requested":True,"refill_reason":reason,"refill_approved":False})
    _log("Refill Request", name, qkey+"/"+channel+" — "+reason)
    _send_email(subject="Refill Request — "+name+"/"+channel+"/"+qkey,
                body=name+" ("+email+") requests refill for "+qkey+"/"+channel+".\nReason: "+reason+"\n\n— forecast.wbn")
    return jsonify({"status":"requested"})

# ── API: Download template ─────────────────────────────────────────────────────
@app.route("/api/download-template/<qkey>")
@_require_login
def api_download_template(qkey):
    if qkey not in QUARTERS: return jsonify({"error":"Invalid quarter"}), 400
    buf = io.BytesIO(); _create_quarter_template(qkey, buf); buf.seek(0)
    return send_file(buf, as_attachment=True, download_name="Forecast_Template_"+qkey+".xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── API: Upload filled template ────────────────────────────────────────────────
@app.route("/api/upload-template/<qkey>", methods=["POST"])
@_require_login
def api_upload_template(qkey):
    if qkey not in QUARTERS: return jsonify({"error":"Invalid quarter"}), 400
    if "file" not in request.files: return jsonify({"error":"No file"}), 400
    f = request.files["file"]
    if not f.filename.endswith((".xlsx",".xls")): return jsonify({"error":"Only .xlsx/.xls"}), 400
    try:
        rows = _load_quarter_template(qkey, io.BytesIO(f.read()))
        return jsonify({"rows":rows,"months":QUARTERS[qkey]["months"]})
    except Exception as e: return jsonify({"error":str(e)}), 400

# ── Admin panel ────────────────────────────────────────────────────────────────
@app.route("/admin")
@_require_admin
def admin_panel():
    with _lock: log = list(reversed(_admin_log[-50:]))
    q_info = {}
    for qkey, qmeta in QUARTERS.items():
        q = _q_get(qkey); subs = _all_subs(qkey) if q.get("initiated") else {}
        q_info[qkey] = {
            "label": qmeta["label"], "initiated": q.get("initiated",False),
            "initiated_at": q.get("initiated_at",""), "sku_count": len(q.get("drr_data") or []),
            "submitted_count": sum(1 for s in subs.values() if s.get("submitted")),
            "refill_count": sum(1 for s in subs.values() if s.get("refill_requested") and not s.get("refill_approved")),
            "channels": {c: {
                "submitted": subs.get(c,{}).get("submitted",False),
                "submitted_at": subs.get(c,{}).get("submitted_at",""),
                "revision": subs.get(c,{}).get("revision",0),
                "user_name": subs.get(c,{}).get("user_name",""),
                "refill_requested": subs.get(c,{}).get("refill_requested",False),
                "refill_reason": subs.get(c,{}).get("refill_reason",""),
            } for c in CHANNELS},
        }
    with _lock:
        all_subs_flat = []
        for qk, chs in _submissions.items():
            for ch, s in chs.items():
                all_subs_flat.append(dict(qkey=qk, channel=ch, **s))
    return render_template("admin.html",
        user_name=_name(), today=datetime.date.today().strftime("%A, %d %B %Y"),
        quarters=QUARTERS, q_info=q_info, channels=CHANNELS,
        total_submitted=sum(1 for s in all_subs_flat if s.get("submitted")),
        pending_refills=sum(1 for s in all_subs_flat if s.get("refill_requested") and not s.get("refill_approved")),
        activity_log=log)

@app.route("/admin/api/initiate-quarter", methods=["POST"])
@_require_admin
def admin_initiate_quarter():
    qkey = request.form.get("quarter","")
    if qkey not in QUARTERS: return jsonify({"error":"Invalid quarter"}), 400
    if "file" not in request.files: return jsonify({"error":"No file"}), 400
    f = request.files["file"]
    if not f.filename.endswith((".xlsx",".xls")): return jsonify({"error":"Only .xlsx/.xls"}), 400
    try: drr_data, channels_found = _parse_drr_excel(io.BytesIO(f.read()))
    except Exception as e: return jsonify({"error":str(e)}), 400
    _q_set(qkey, {"initiated":True, "initiated_at":datetime.date.today().strftime("%d %b %Y"),
                   "drr_data":drr_data, "channels_found":channels_found})
    _log("Quarter Initiated", _name(), qkey+" — "+str(len(drr_data))+" SKUs")
    return jsonify({"status":"ok","sku_count":len(drr_data),"channels_found":channels_found})

@app.route("/admin/api/revoke-quarter", methods=["POST"])
@_require_admin
def admin_revoke_quarter():
    qkey = (request.json or {}).get("quarter","")
    if qkey not in QUARTERS: return jsonify({"error":"Invalid"}), 400
    _q_revoke(qkey); _log("Quarter Revoked", _name(), qkey)
    return jsonify({"status":"ok"})

@app.route("/admin/api/reset-submission", methods=["POST"])
@_require_admin
def admin_reset_submission():
    data = request.json or {}
    qkey = data.get("quarter",""); channel = data.get("channel","")
    if qkey not in QUARTERS or channel not in CHANNELS: return jsonify({"error":"Invalid"}), 400
    _sub_reset(qkey, channel); _log("Submission Reset", _name(), qkey+"/"+channel)
    return jsonify({"status":"ok"})

@app.route("/admin/api/approve-refill", methods=["POST"])
@_require_admin
def admin_approve_refill():
    data = request.json or {}; qkey = data.get("quarter",""); channel = data.get("channel","")
    sub = _sub_get(qkey, channel)
    if not sub.get("refill_requested"): return jsonify({"error":"No pending request"}), 400
    _sub_set(qkey, channel, {"submitted":False,"refill_approved":True,"refill_requested":False})
    _log("Refill Approved", _name(), qkey+"/"+channel)
    return jsonify({"status":"approved"})

@app.route("/admin/api/deny-refill", methods=["POST"])
@_require_admin
def admin_deny_refill():
    data = request.json or {}; qkey = data.get("quarter",""); channel = data.get("channel","")
    _sub_set(qkey, channel, {"refill_requested":False,"refill_reason":"","refill_approved":False})
    _log("Refill Denied", _name(), qkey+"/"+channel)
    return jsonify({"status":"denied"})

@app.route("/admin/api/force-submit", methods=["POST"])
@_require_admin
def admin_force_submit():
    data = request.json or {}; qkey = data.get("quarter",""); channel = data.get("channel","")
    if qkey not in QUARTERS or channel not in CHANNELS: return jsonify({"error":"Invalid"}), 400
    at = datetime.datetime.now().strftime("%d %b %Y, %H:%M")
    _sub_set(qkey, channel, {"submitted":True,"submitted_at":at,"user_name":"Admin ("+_name()+")","revision":1})
    _log("Force Submit", _name(), qkey+"/"+channel)
    return jsonify({"status":"ok","submitted_at":at})

@app.route("/admin/api/export-quarter/<qkey>")
@_require_admin
def admin_export_quarter(qkey):
    if qkey not in QUARTERS: return jsonify({"error":"Invalid"}), 400
    q = _q_get(qkey)
    if not q.get("initiated"): return jsonify({"error":"Not initiated"}), 404
    buf = io.BytesIO(); _export_quarter_excel(qkey, q, buf); buf.seek(0)
    fn = "DRR_Forecast_"+qkey+"_"+datetime.date.today().strftime("%Y-%m-%d")+".xlsx"
    return send_file(buf, as_attachment=True, download_name=fn,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.route("/admin/api/download-submission/<qkey>/<channel>")
@_require_admin
def admin_download_sub(qkey, channel):
    sub = _sub_get(qkey, channel); eb = sub.get("excel_bytes")
    if not eb: return jsonify({"error":"No file"}), 404
    buf = io.BytesIO(eb); buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=sub.get("file","submission.xlsx"),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── Excel helpers ──────────────────────────────────────────────────────────────
def _parse_drr_excel(source):
    """
    Parse the channel-wise DRR Excel (2-row header format).
    Row 1: channel names (merged across 5 DRR columns each)
    Row 2: column labels (Category, SKU, etc. + DRR label names)
    Returns: (list of row dicts with _drr embedded, list of channels found)
    """
    import openpyxl
    wb = openpyxl.load_workbook(source, read_only=True, data_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    if len(all_rows) < 3:
        raise ValueError("Sheet too small — need 2 header rows + data.")

    h0 = [str(c or "").strip() for c in all_rows[0]]
    h1 = [str(c or "").strip() for c in all_rows[1]]

    # Map base column names to indices
    base_idx = {}
    for col in BASE_COLS_DRR:
        try: base_idx[col] = h1.index(col)
        except ValueError: pass

    # Build column map: col_index -> {ch, label}
    col_map = {}
    cur_ch  = None
    for c, cv in enumerate(h0):
        if cv: cur_ch = cv
        lbl = h1[c] if c < len(h1) else ""
        if lbl in DRR_LABELS and cur_ch:
            col_map[c] = {"ch": cur_ch, "label": lbl}

    rows = []
    for ri, raw in enumerate(all_rows[2:], start=2):
        row_str = [str(c or "").strip() for c in raw]
        sku  = row_str[base_idx["SKU"]] if "SKU" in base_idx and base_idx["SKU"] < len(row_str) else ""
        name = row_str[base_idx["Product Name"]] if "Product Name" in base_idx and base_idx["Product Name"] < len(row_str) else ""
        if not sku and not name:
            continue

        obj = {"_row_id": "r"+str(ri)}
        for col in BASE_COLS_DRR:
            obj[col] = row_str[base_idx[col]] if col in base_idx and base_idx[col] < len(row_str) else ""

        obj["_drr"] = {}
        for c, info in col_map.items():
            ch_name = info["ch"]
            if ch_name not in obj["_drr"]:
                obj["_drr"][ch_name] = {}
            try:
                val = raw[c] if c < len(raw) else None
                obj["_drr"][ch_name][info["label"]] = round(float(val or 0), 2)
            except (TypeError, ValueError):
                obj["_drr"][ch_name][info["label"]] = 0.0
        rows.append(obj)

    channels_found = list({info["ch"] for info in col_map.values()})
    return rows, channels_found

def _create_quarter_template(qkey, dest):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    months = QUARTERS[qkey]["months"]
    bdr = Border(**{s: Side(style="thin", color="CCCCCC") for s in ["left","right","top","bottom"]})
    wb = Workbook(); ws = wb.active; ws.title = "Forecast "+qkey
    headers = ["Category","Sub-Category","Product Type","Product Name","SKU"] + months
    for ci, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.font = Font(bold=True, color="FFFFFF", name="Calibri", size=10)
        cell.fill = PatternFill(start_color="1B4332", end_color="1B4332", fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = bdr
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 28
    ws.column_dimensions["E"].width = 16
    for i in range(6, len(headers)+1):
        ws.column_dimensions[get_column_letter(i)].width = 14
    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "F2"
    wb.save(dest)

def _load_quarter_template(qkey, source):
    import openpyxl
    months = QUARTERS[qkey]["months"]
    wb = openpyxl.load_workbook(source, read_only=True, data_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    if len(all_rows) < 2: raise ValueError("Template appears empty.")
    h = [str(c or "").strip() for c in all_rows[0]]
    rows = []
    for ri, raw in enumerate(all_rows[1:], start=1):
        row = {h[ci]: (str(c).strip() if c is not None else "") for ci, c in enumerate(raw) if ci < len(h)}
        sku = row.get("SKU","") or row.get("Product Name","")
        if not sku: continue
        row["_row_id"] = "r"+str(ri)
        row["_ref"]    = {dl: 0 for dl in DRR_LABELS}
        for m in months: row.setdefault(m, "")
        rows.append(row)
    return rows

def _save_channel_excel(rows, username, channel, qkey, dest):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    months = QUARTERS[qkey]["months"]
    bdr = Border(**{s: Side(style="thin", color="DEE2E6") for s in ["left","right","top","bottom"]})
    wb = Workbook(); ws = wb.active; ws.title = "Forecast Submission"

    # Title row
    total_cols = len(BASE_COLS_DRR) + len(DRR_LABELS) + len(months)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=total_cols)
    tc = ws.cell(row=1, column=1,
                 value="Forecast — "+qkey+" — "+channel+" — "+username+" — "+datetime.date.today().strftime("%d %b %Y"))
    tc.font = Font(bold=True, color="FFFFFF", name="Calibri", size=12)
    tc.fill = PatternFill(start_color="1B4332", end_color="1B4332", fill_type="solid")
    tc.alignment = Alignment(horizontal="left", vertical="center", indent=1)
    ws.row_dimensions[1].height = 28

    # Header
    headers = BASE_COLS_DRR + DRR_LABELS + months
    for ci, h in enumerate(headers, 1):
        cell = ws.cell(row=2, column=ci, value=h)
        cell.font = Font(bold=True, color="FFFFFF", name="Calibri", size=9)
        is_drr   = ci > len(BASE_COLS_DRR) and ci <= len(BASE_COLS_DRR)+len(DRR_LABELS)
        is_month = ci > len(BASE_COLS_DRR)+len(DRR_LABELS)
        bg = "2D6A4F" if is_drr else ("40916C" if is_month else "1B4332")
        cell.fill = PatternFill(start_color=bg, end_color=bg, fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = bdr
    ws.row_dimensions[2].height = 28

    for ri, row in enumerate(rows, 3):
        even = ri % 2 == 0
        ref  = row.get("_ref", {})
        all_vals = (
            [row.get(h,"") for h in BASE_COLS_DRR] +
            [ref.get(dl,"") for dl in DRR_LABELS] +
            [row.get(m,"") for m in months]
        )
        for ci, val in enumerate(all_vals, 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = bdr
            is_drr   = ci > len(BASE_COLS_DRR) and ci <= len(BASE_COLS_DRR)+len(DRR_LABELS)
            is_month = ci > len(BASE_COLS_DRR)+len(DRR_LABELS)
            if ci <= len(BASE_COLS_DRR):
                bg = "D8F3DC"
            elif is_drr:
                bg = "E0E7FF" if even else "EEF2FF"
            else:
                bg = "DCFCE7" if even else "F0FFF4"
            cell.fill = PatternFill(start_color=bg, end_color=bg, fill_type="solid")
            cell.font = Font(name="Calibri", size=9)
        ws.row_dimensions[ri].height = 18

    for ci in range(1, len(BASE_COLS_DRR)+1):
        ws.column_dimensions[get_column_letter(ci)].width = 16
    for ci in range(len(BASE_COLS_DRR)+1, len(BASE_COLS_DRR)+len(DRR_LABELS)+1):
        ws.column_dimensions[get_column_letter(ci)].width = 14
    for ci in range(len(BASE_COLS_DRR)+len(DRR_LABELS)+1, len(headers)+1):
        ws.column_dimensions[get_column_letter(ci)].width = 14

    ws.freeze_panes = get_column_letter(len(BASE_COLS_DRR)+1)+"3"
    wb.save(dest)

def _export_quarter_excel(qkey, q, dest):
    from openpyxl import Workbook
    months   = QUARTERS[qkey]["months"]
    drr_data = q.get("drr_data") or []
    wb = Workbook(); ws_sum = wb.active; ws_sum.title = "Summary"
    ws_sum.append(["Quarter", qkey, QUARTERS[qkey]["label"]])
    ws_sum.append(["Exported", datetime.date.today().isoformat()])
    ws_sum.append([])
    ws_sum.append(["Channel","Status","User","Submitted At","Revision"])
    for ch in CHANNELS:
        sub = _sub_get(qkey, ch)
        ws_sum.append([ch, "Submitted" if sub.get("submitted") else "Pending",
                        sub.get("user_name",""), sub.get("submitted_at",""), sub.get("revision",0)])
    for ch in CHANNELS:
        sub = _sub_get(qkey, ch); saved = sub.get("data") or {}
        ws = wb.create_sheet(ch[:31])
        ws.append(["Product Name","SKU","Category","Sub-Category"]+DRR_LABELS+months)
        for row in drr_data:
            rid = row.get("_row_id",""); drr = row.get("_drr",{}).get(ch,{})
            r = [row.get("Product Name",""), row.get("SKU",""),
                 row.get("Category",""), row.get("Sub-Category","")]
            r += [drr.get(dl,0) for dl in DRR_LABELS]
            r += [saved.get(rid,{}).get(m,"") for m in months]
            ws.append(r)
    wb.save(dest)

if __name__ == "__main__":
    print("\n  forecast.wbn — Wellbeing Nutrition")
    print("  http://localhost:5000\n")
    app.run(host="0.0.0.0", port=5000, debug=True)
