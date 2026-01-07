import os
import json
import time
import math
import random
import datetime as dt
from zoneinfo import ZoneInfo

import requests
import psycopg2
from psycopg2.extras import execute_values


WB_PROMO_TOKEN = os.getenv("WB_ADVERT_TOKEN", "").strip()
SUPABASE_DSN = os.getenv("SUPABASE_DSN", "").strip()

DAYS_BACK = int(os.getenv("DAYS_BACK", "7"))  # сколько последних дней грузим (заканчивая вчера)
FULLSTATS_SLEEP_SEC = float(os.getenv("FULLSTATS_SLEEP_SEC", "22"))  # у fullstats лимиты, лучше пауза
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))

MSK = ZoneInfo("Europe/Moscow")

PROMO_BASE = "https://advert-api.wildberries.ru"
URL_PROMO_COUNT = f"{PROMO_BASE}/adv/v1/promotion/count"
URL_ADVERT_V2_ADVERTS = f"{PROMO_BASE}/api/advert/v2/adverts"
URL_FULLSTATS = f"{PROMO_BASE}/adv/v3/fullstats"

STATUSES_ALLOWED = {7, 9, 11}  # fullstats работает для статусов 7/9/11 (completed/active/paused)

HEADERS = {
    "Authorization": WB_PROMO_TOKEN,
    "Content-Type": "application/json",
}


def _die(msg: str):
    raise SystemExit(f"ERROR: {msg}")


def request_json(method: str, url: str, *, params=None, json_body=None, retries: int = 6):
    if not WB_PROMO_TOKEN:
        _die("WB_ADVERT_TOKEN is empty")
    if not SUPABASE_DSN:
        _die("SUPABASE_DSN is empty")

    for attempt in range(1, retries + 1):
        try:
            r = requests.request(
                method=method,
                url=url,
                headers=HEADERS,
                params=params,
                json=json_body,
                timeout=HTTP_TIMEOUT,
            )

            if r.status_code == 429:
                # WB часто возвращает лимиты + можно ориентироваться на retry headers,
                # но на всякий случай — backoff.
                retry_after = None
                for h in ("X-Ratelimit-Retry", "Retry-After"):
                    if h in r.headers:
                        try:
                            retry_after = float(r.headers[h])
                        except Exception:
                            pass
                sleep_s = retry_after if retry_after is not None else (25 + random.random() * 10)
                print(f"⚠️ HTTP 429. Retry {attempt}/{retries} after {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue

            if 500 <= r.status_code <= 599:
                sleep_s = min(60, 5 * attempt + random.random() * 2)
                print(f"⚠️ HTTP {r.status_code}. Retry {attempt}/{retries} after {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue

            r.raise_for_status()
            if r.text.strip() == "":
                return None
            return r.json()

        except Exception as e:
            if attempt >= retries:
                raise
            sleep_s = min(60, 3 * attempt + random.random() * 2)
            print(f"⚠️ {type(e).__name__}: {e}. Retry {attempt}/{retries} after {sleep_s:.1f}s")
            time.sleep(sleep_s)

    return None


def get_campaign_ids():
    """
    /adv/v1/promotion/count -> adverts grouped by type/status -> advert_list[].advertId
    Возвращаем:
      - ids: set[int]
      - meta: dict[advert_id] -> {campaign_type, campaign_status}
    """
    data = request_json("GET", URL_PROMO_COUNT)
    ids = set()
    meta = {}

    for g in (data or {}).get("adverts", []):
        ctype = g.get("type")
        status = g.get("status")
        if status not in STATUSES_ALLOWED:
            continue
        for item in g.get("advert_list", []) or []:
            advert_id = item.get("advertId")
            if advert_id is None:
                continue
            ids.add(int(advert_id))
            meta[int(advert_id)] = {"campaign_type": ctype, "campaign_status": status}

    return ids, meta


def chunked(lst, n):
    lst = list(lst)
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def get_campaign_settings(advert_ids):
    """
    /api/advert/v2/adverts?ids=... -> settings.placements.search/recommendations, settings.payment_type
    Возвращаем dict[advert_id] -> {payment_type, placement_search, placement_recommendations}
    """
    out = {}

    for part in chunked(advert_ids, 50):
        params = {
            "ids": ",".join(str(x) for x in part),
            "statuses": ",".join(str(x) for x in sorted(STATUSES_ALLOWED)),
        }
        data = request_json("GET", URL_ADVERT_V2_ADVERTS, params=params)
        for adv in (data or {}).get("adverts", []):
            adv_id = adv.get("id")
            settings = adv.get("settings") or {}
            placements = settings.get("placements") or {}

            out[int(adv_id)] = {
                "payment_type": settings.get("payment_type"),
                "placement_search": bool(placements.get("search")) if "search" in placements else None,
                "placement_recommendations": bool(placements.get("recommendations")) if "recommendations" in placements else None,
            }

        # лимит у этого метода мягкий, но не долбим зря
        time.sleep(0.25)

    return out


def placement_scope(search_flag, recom_flag):
    if search_flag is True and recom_flag is True:
        return "search+recommendations"
    if search_flag is True and (recom_flag is False or recom_flag is None):
        return "search"
    if recom_flag is True and (search_flag is False or search_flag is None):
        return "recommendations"
    if search_flag is False and recom_flag is False:
        return "none"
    return "unknown"


def parse_report_date(day_obj):
    # в sample у WB date бывает "2025-09-07T00:00:00Z" -> берём первые 10 символов
    s = (day_obj.get("date") or "")[:10]
    return dt.date.fromisoformat(s)


def upsert_rows(rows):
    if not rows:
        return 0

    sql = """
    insert into public.wb_adv_fullstats_daily_raw (
        report_date,
        advert_id,
        campaign_type,
        campaign_status,
        payment_type,
        placement_search,
        placement_recommendations,
        placement_scope,
        raw_item,
        load_dttm
    )
    values %s
    on conflict (report_date, advert_id)
    do update set
        campaign_type = excluded.campaign_type,
        campaign_status = excluded.campaign_status,
        payment_type = excluded.payment_type,
        placement_search = excluded.placement_search,
        placement_recommendations = excluded.placement_recommendations,
        placement_scope = excluded.placement_scope,
        raw_item = excluded.raw_item,
        load_dttm = excluded.load_dttm
    ;
    """

    with psycopg2.connect(SUPABASE_DSN) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=200)
    return len(rows)


def main():
    today_msk = dt.datetime.now(MSK).date()
    end_date = today_msk - dt.timedelta(days=1)  # льём “вчера” как последнюю полную дату
    begin_date = end_date - dt.timedelta(days=DAYS_BACK - 1)

    print(f"MSK today: {today_msk} | Range: {begin_date}..{end_date} (DAYS_BACK={DAYS_BACK})")

    advert_ids, meta_map = get_campaign_ids()
    advert_ids = sorted(advert_ids)

    if not advert_ids:
        print("No campaigns found in statuses 7/9/11 — nothing to load.")
        return

    print(f"Campaigns found: {len(advert_ids)}")

    settings_map = get_campaign_settings(advert_ids)

    total_upserted = 0

    for part in chunked(advert_ids, 50):
        params = {
            "ids": ",".join(str(x) for x in part),
            "beginDate": begin_date.isoformat(),
            "endDate": end_date.isoformat(),
        }

        print(f"Fetch fullstats: ids={len(part)} beginDate={params['beginDate']} endDate={params['endDate']}")
        data = request_json("GET", URL_FULLSTATS, params=params)

        rows = []
        now_ts = dt.datetime.now(dt.timezone.utc)

        for adv in data or []:
            advert_id = int(adv.get("advertId"))
            days = adv.get("days") or []
            booster_stats = adv.get("boosterStats") or []

            m = meta_map.get(advert_id, {})
            s = settings_map.get(advert_id, {})

            p_search = s.get("placement_search")
            p_recom = s.get("placement_recommendations")

            for day in days:
                d = parse_report_date(day)
                if d < begin_date or d > end_date:
                    continue

                booster_for_day = []
                for bs in booster_stats:
                    # boosterStats.date в sample бывает "2025-09-07"
                    bs_date = (bs.get("date") or "")[:10]
                    if bs_date == d.isoformat():
                        booster_for_day.append(bs)

                raw_item = {
                    "advertId": advert_id,
                    "day": day,
                    "boosterStats": booster_for_day,
                    "loadedRange": {"beginDate": begin_date.isoformat(), "endDate": end_date.isoformat()},
                }

                rows.append((
                    d,
                    advert_id,
                    m.get("campaign_type"),
                    m.get("campaign_status"),
                    s.get("payment_type"),
                    p_search,
                    p_recom,
                    placement_scope(p_search, p_recom),
                    json.dumps(raw_item, ensure_ascii=False),
                    now_ts,
                ))

        upserted = upsert_rows(rows)
        total_upserted += upserted
        print(f"Upserted rows: {upserted}")

        # у fullstats жёсткие лимиты — не долбим часто
        time.sleep(FULLSTATS_SLEEP_SEC)

    print(f"DONE. Total upserted: {total_upserted}")


if __name__ == "__main__":
    main()
