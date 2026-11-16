"""
Gemini AI integration for personalized email drafts and reply scenarios.
Uses the REST API directly (no SDK version dependency).
Falls back to template-based content when no API key is configured.
"""
import json
import re
import sqlite3
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone

_api_key = None
_configured = False
_last_error = None
_model_name = "gemini-3.1-flash-lite"
_api_base = "https://generativelanguage.googleapis.com/v1beta"


def configure(api_key):
    global _api_key, _configured, _last_error
    try:
        _api_key = api_key
        result = _call_api("Reply with exactly the word OK", max_tokens=32, temp=0)
        if result:
            _configured = True
            _last_error = None
            return True
        _last_error = "API returned empty response"
        _configured = False
        return _last_error
    except Exception as e:
        _configured = False
        _api_key = None
        _last_error = str(e)
        return _last_error


def is_configured():
    return _configured and _api_key is not None


def get_last_error():
    return _last_error


def _call_api(prompt, max_tokens=1024, temp=0.7):
    global _last_error
    url = f"{_api_base}/models/{_model_name}:generateContent?key={_api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temp,
            "maxOutputTokens": max_tokens,
        }
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
        candidates = body.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                return parts[0].get("text", "")
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()[:300]
        _last_error = f"{e.code} {err_body}"
        raise
    except Exception as e:
        _last_error = str(e)
        raise
    return None


def _parse_json(text):
    if not text:
        return None
    text = text.strip()
    m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    m = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    m = re.search(r'\{.*\}', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    return None


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


REPLY_CATEGORIES = [
    ("INTERESTED", "Yes, I'd love to learn more. Let's schedule a meeting.",
     "Confirm interest, suggest 2-3 specific meeting times, express enthusiasm"),
    ("QUESTION", "Can you tell me more about how the referral program works and what's involved?",
     "Answer helpfully with an overview, offer a call to explain details"),
    ("NOT_NOW", "Not the right time for us. Maybe reach out again next quarter.",
     "Respect timing, acknowledge, offer to follow up at their preferred time"),
    ("OUT_OF_OFFICE", "I'm currently on leave until the 15th. Will respond when I'm back.",
     "Acknowledge absence, plan to follow up after their return date"),
    ("WRONG_PERSON", "I'm not the right person for this. You might want to contact our director.",
     "Thank them, politely ask for the right contact's details"),
    ("UNSUBSCRIBE", "Please remove me from your mailing list.",
     "Confirm immediate removal, apologise for any inconvenience"),
    ("NEEDS_REVIEW", "I need to discuss this with my business partner before making any decisions.",
     "Acknowledge, offer to provide materials they can share, suggest a group meeting"),
]


def generate_outreach(lead, conn=None):
    cache_key = f"{lead['lead_id']}:outreach"
    if conn:
        cached = conn.execute("SELECT subject, body FROM ai_content WHERE id=?", (cache_key,)).fetchone()
        if cached:
            return {"subject": cached["subject"], "body": cached["body"], "cached": True}

    if not is_configured():
        return _template_outreach(lead)

    seg_labels = {
        "MORTGAGE_BROKER": "mortgage brokers",
        "ACCOUNTANT": "accountants",
        "FINANCIAL_PLANNER": "financial planners",
    }
    seg_audience = seg_labels.get(lead.get("segment", ""), "professionals")

    prompt = f"""You are a business development representative at Cashback Property, an Australian company
that partners with mortgage brokers, accountants, and financial planners as referral partners.

Write a personalized cold outreach email to:
- Name: {lead['first_name']} {lead['last_name']}
- Company: {lead['company']}
- Role: {lead.get('role_norm', lead.get('role', ''))}
- City: {lead.get('city_norm', lead.get('city', ''))}
- They are in the "{seg_audience}" segment

Requirements:
- Professional, warm Australian business tone
- Reference their specific company name and role naturally
- Briefly explain Cashback Property's referral partnership for {seg_audience}
- Include a clear call-to-action for a 15-minute introductory meeting
- End with: "If you'd rather not hear from us, reply 'unsubscribe' or use {{{{OPT_OUT_LINK}}}} and we'll remove you straight away."
- Sign off with: "Kind regards,\\n{{{{SENDER_NAME}}}}\\nCashback Property\\n{{{{SENDER_BUSINESS_DETAILS}}}}"
- Do NOT make claims about specific commission rates, guaranteed returns, or income
- Do NOT fake prior familiarity or invent shared connections
- Keep the body under 150 words
- Subject line should be concise and relevant

Return ONLY valid JSON: {{"subject": "the subject line", "body": "the email body"}}"""

    try:
        raw = _call_api(prompt)
        print(f"[Gemini outreach {lead['lead_id']}] response length={len(raw or '')}", file=sys.stderr)
        data = _parse_json(raw)
        if data and "subject" in data and "body" in data:
            if conn:
                _cache(conn, cache_key, lead["lead_id"], "outreach", data["subject"], data["body"])
            return {"subject": data["subject"], "body": data["body"], "cached": False}
        else:
            print(f"[Gemini outreach {lead['lead_id']}] JSON parse failed. Raw: {(raw or '')[:500]}", file=sys.stderr)
    except Exception as e:
        print(f"[Gemini outreach {lead['lead_id']}] ERROR: {e}", file=sys.stderr)
    return _template_outreach(lead)


def generate_reply_scenario(lead, category, conn=None):
    cache_key = f"{lead['lead_id']}:reply_{category}"
    if conn:
        cached = conn.execute(
            "SELECT subject, body, sample_incoming FROM ai_content WHERE id=?", (cache_key,)
        ).fetchone()
        if cached:
            return {
                "subject": cached["subject"], "body": cached["body"],
                "sample_incoming": cached["sample_incoming"], "cached": True
            }

    cat_info = next((c for c in REPLY_CATEGORIES if c[0] == category), None)
    if not cat_info:
        return None
    _, sample_reply, guidance = cat_info

    if not is_configured():
        return _template_reply(lead, category, sample_reply)

    prompt = f"""You are responding on behalf of Cashback Property (Australian company) to a reply from a prospect.

Prospect info:
- Name: {lead['first_name']} {lead['last_name']}
- Company: {lead['company']}
- Role: {lead.get('role_norm', lead.get('role', ''))}
- City: {lead.get('city_norm', lead.get('city', ''))}

The original outreach was about a referral partnership and a 15-minute introductory meeting.

They replied with a "{category}" type response.
Sample of what they might say: "{sample_reply}"
Guidance: {guidance}

Write TWO things:
1. A realistic sample incoming reply they might send (1-2 sentences, natural tone)
2. Your professional response email (subject + body)

Rules for the response:
- Professional, warm Australian tone
- Use their first name
- Reference their company where natural
- For UNSUBSCRIBE: confirm removal, no sales pitch
- Never make false claims or guarantee returns
- Keep response body under 120 words
- Sign off with: "Kind regards,\\n{{{{SENDER_NAME}}}}\\nCashback Property"

Return ONLY valid JSON: {{"sample_incoming": "what they said", "subject": "response subject", "body": "response body"}}"""

    try:
        raw = _call_api(prompt)
        print(f"[Gemini reply {lead['lead_id']}:{category}] response length={len(raw or '')}", file=sys.stderr)
        data = _parse_json(raw)
        if data and "subject" in data and "body" in data:
            data.setdefault("sample_incoming", sample_reply)
            if conn:
                _cache(conn, cache_key, lead["lead_id"], f"reply_{category}",
                       data["subject"], data["body"], data["sample_incoming"])
            data["cached"] = False
            return data
        else:
            print(f"[Gemini reply {lead['lead_id']}:{category}] JSON parse failed. Raw: {(raw or '')[:500]}", file=sys.stderr)
    except Exception as e:
        print(f"[Gemini reply {lead['lead_id']}:{category}] ERROR: {e}", file=sys.stderr)
    return _template_reply(lead, category, sample_reply)


def generate_all_scenarios(lead, conn=None):
    results = {}
    for cat, _, _ in REPLY_CATEGORIES:
        results[cat] = generate_reply_scenario(lead, cat, conn)
    return results


def _cache(conn, cache_key, lead_id, content_type, subject, body, sample_incoming=""):
    conn.execute(
        "INSERT OR REPLACE INTO ai_content VALUES(?,?,?,?,?,?,?,?)",
        (cache_key, lead_id, content_type, subject, body, sample_incoming, _model_name, _now())
    )
    conn.commit()


def clear_cache(conn, lead_id=None):
    if lead_id:
        conn.execute("DELETE FROM ai_content WHERE lead_id=?", (lead_id,))
    else:
        conn.execute("DELETE FROM ai_content")
    conn.commit()


def _template_outreach(lead):
    seg_labels = {
        "MORTGAGE_BROKER": ("Mortgage Broker", "mortgage brokers"),
        "ACCOUNTANT": ("Accountant", "accountants"),
        "FINANCIAL_PLANNER": ("Financial Planner", "financial planners"),
    }
    role, audience = seg_labels.get(lead.get("segment", ""), ("professional", "professionals"))
    first = lead["first_name"]
    company = lead["company"]

    return {
        "subject": f"Referral partnership – 15 minutes, {company}?",
        "body": (
            f"Hi {first},\n\n"
            f"I'm writing from Cashback Property. We're looking to work with Australian {audience} "
            f"as referral partners, and I'd like to explain how our referral program works and "
            f"whether it could suit {company}'s clients.\n\n"
            f"Would you be open to a 15-minute meeting next week? Happy to work around your calendar.\n\n"
            f"Kind regards,\n{{{{SENDER_NAME}}}}\nCashback Property\n{{{{SENDER_BUSINESS_DETAILS}}}}\n\n"
            f"If you'd rather not hear from us, reply 'unsubscribe' or use {{{{OPT_OUT_LINK}}}} "
            f"and we'll remove you straight away."
        ),
        "cached": False, "template": True
    }


def _template_reply(lead, category, sample_reply):
    first = lead["first_name"]
    company = lead["company"]
    responses = {
        "INTERESTED": {
            "subject": f"Re: Referral partnership – {company}",
            "body": f"Hi {first},\n\nGreat to hear you're interested! I'd love to walk you through how our referral program works.\n\nWould any of these work for a quick 15-minute call?\n- [Time option 1]\n- [Time option 2]\n- [Time option 3]\n\nHappy to work around your schedule.\n\nKind regards,\n{{{{SENDER_NAME}}}}\nCashback Property"
        },
        "QUESTION": {
            "subject": f"Re: More info on the referral program – {company}",
            "body": f"Hi {first},\n\nThanks for your question. Happy to provide more details.\n\nOur referral partnership is designed for {company} and similar firms – it's straightforward and doesn't require any additional licensing.\n\nWould a brief call be easier? I can walk through the details in 10-15 minutes and answer any questions.\n\nKind regards,\n{{{{SENDER_NAME}}}}\nCashback Property"
        },
        "NOT_NOW": {
            "subject": f"Re: Timing – {company}",
            "body": f"Hi {first},\n\nCompletely understand – timing is everything. I'll make a note to follow up at a time that works better for you.\n\nWould it be alright if I reached out again next quarter?\n\nKind regards,\n{{{{SENDER_NAME}}}}\nCashback Property"
        },
        "OUT_OF_OFFICE": {
            "subject": f"Re: Follow up – {company}",
            "body": f"Hi {first},\n\nThanks for the auto-reply. I'll follow up after you're back in the office.\n\nNo action needed on your end – I'll be in touch then.\n\nKind regards,\n{{{{SENDER_NAME}}}}\nCashback Property"
        },
        "WRONG_PERSON": {
            "subject": f"Re: Right contact at {company}?",
            "body": f"Hi {first},\n\nApologies for the misdirect, and thanks for letting me know. Could you point me to the right person at {company} who handles referral partnerships or business development?\n\nAppreciate the help.\n\nKind regards,\n{{{{SENDER_NAME}}}}\nCashback Property"
        },
        "UNSUBSCRIBE": {
            "subject": f"Re: Removal confirmed",
            "body": f"Hi {first},\n\nDone – I've removed your details and you won't hear from us again.\n\nApologies for any inconvenience.\n\nKind regards,\n{{{{SENDER_NAME}}}}\nCashback Property"
        },
        "NEEDS_REVIEW": {
            "subject": f"Re: {company} – referral partnership",
            "body": f"Hi {first},\n\nThanks for getting back to me. I understand you need time to consider.\n\nI'm happy to send through a brief overview you can share with your team, or we could set up a short call with whoever else would need to be involved.\n\nNo rush – let me know what works best.\n\nKind regards,\n{{{{SENDER_NAME}}}}\nCashback Property"
        },
    }
    r = responses.get(category, responses["NEEDS_REVIEW"])
    return {"subject": r["subject"], "body": r["body"],
            "sample_incoming": sample_reply, "cached": False, "template": True}
