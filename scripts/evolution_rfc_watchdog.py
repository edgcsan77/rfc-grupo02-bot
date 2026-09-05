import re
import sys
from pathlib import Path

import requests
from dotenv import dotenv_values


BASE_DIR = Path("/opt/rfc-grupo02-bot")
CFG = dotenv_values(BASE_DIR / ".env")

BASE = str(
    CFG.get("EVOLUTION_BASE_URL")
    or "http://127.0.0.1:8080"
).rstrip("/")

API_KEY = str(
    CFG.get("EVOLUTION_API_KEY")
    or ""
).strip()

EXPECTED_URL = (
    "https://panel.rfc.docifymx.com/"
    "webhook/evolution-rfc"
)

HEADERS = {
    "apikey": API_KEY,
    "Content-Type": "application/json",
}


def is_rfc_instance(name: str) -> bool:
    name = str(name or "").strip().lower()

    if name == "docifybot8mx":
        return True

    return bool(
        re.fullmatch(
            r"grupo\d+",
            name,
        )
    )


def instance_name(row):
    if not isinstance(row, dict):
        return ""

    nested = row.get("instance")

    if not isinstance(nested, dict):
        nested = {}

    return str(
        row.get("name")
        or row.get("instanceName")
        or nested.get("instanceName")
        or ""
    ).strip()


def get_instances():
    r = requests.get(
        BASE + "/instance/fetchInstances",
        headers=HEADERS,
        timeout=30,
    )

    r.raise_for_status()

    data = r.json()

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        rows = (
            data.get("instances")
            or data.get("data")
            or []
        )

        if isinstance(rows, list):
            return rows

    return []


def get_state(name):
    try:
        r = requests.get(
            f"{BASE}/instance/connectionState/{name}",
            headers=HEADERS,
            timeout=10,
        )

        if not r.ok:
            return f"HTTP_{r.status_code}"

        data = r.json()

        if isinstance(data, dict):
            inst = data.get("instance") or {}

            if isinstance(inst, dict):
                return str(
                    inst.get("state")
                    or "unknown"
                )

            return str(
                data.get("state")
                or "unknown"
            )

    except Exception as exc:
        return (
            "ERROR_"
            + type(exc).__name__
        )

    return "unknown"


def get_webhook(name):
    r = requests.get(
        f"{BASE}/webhook/find/{name}",
        headers=HEADERS,
        timeout=10,
    )

    if not r.ok:
        raise RuntimeError(
            f"WEBHOOK_FIND_HTTP_{r.status_code}:"
            f"{r.text[:500]}"
        )

    data = r.json()

    if not isinstance(data, dict):
        raise RuntimeError(
            "WEBHOOK_FIND_BAD_JSON"
        )

    return data


def webhook_ok(data):
    events = [
        str(x).upper()
        for x in (
            data.get("events")
            or []
        )
    ]

    return (
        data.get("enabled") is True
        and str(
            data.get("url")
            or ""
        ).rstrip("/")
        == EXPECTED_URL.rstrip("/")
        and "MESSAGES_UPSERT"
        in events
        and not bool(
            data.get("webhookByEvents")
        )
    )


def repair_webhook(name, current):
    events = []

    for value in (
        current.get("events")
        or []
    ):
        value = str(value).upper().strip()

        if value and value not in events:
            events.append(value)

    if "MESSAGES_UPSERT" not in events:
        events.append(
            "MESSAGES_UPSERT"
        )

    # Si la configuración quedó totalmente vacía
    # después de un restart, dejamos únicamente
    # el evento requerido por RFC.
    if not events:
        events = [
            "MESSAGES_UPSERT"
        ]

    payload = {
        "webhook": {
            "enabled": True,
            "url": EXPECTED_URL,
            "webhookByEvents": False,
            "webhookBase64": False,
            "events": events,
        }
    }

    r = requests.post(
        f"{BASE}/webhook/set/{name}",
        headers=HEADERS,
        json=payload,
        timeout=20,
    )

    if r.status_code not in (
        200,
        201,
    ):
        raise RuntimeError(
            f"WEBHOOK_SET_HTTP_{r.status_code}:"
            f"{r.text[:1000]}"
        )

    check = get_webhook(name)

    if not webhook_ok(check):
        raise RuntimeError(
            "WEBHOOK_REPAIR_NOT_CONFIRMED:"
            + repr(check)
        )

    return check


def main():
    failures = 0
    rows = get_instances()

    rfc_names = sorted(
        {
            instance_name(row)
            for row in rows
            if is_rfc_instance(
                instance_name(row)
            )
        }
    )

    print(
        "RFC_EVOLUTION_WATCHDOG_START =",
        {
            "total_rfc_instances":
                len(rfc_names),
        },
        flush=True,
    )

    for name in rfc_names:
        state = get_state(name)

        try:
            current = get_webhook(
                name
            )

            if webhook_ok(current):
                print(
                    "RFC_EVOLUTION_OK =",
                    {
                        "instance": name,
                        "state": state,
                    },
                    flush=True,
                )
                continue

            print(
                "RFC_EVOLUTION_WEBHOOK_DRIFT =",
                {
                    "instance": name,
                    "state": state,
                    "enabled":
                        current.get(
                            "enabled"
                        ),
                    "url":
                        current.get(
                            "url"
                        ),
                    "events":
                        current.get(
                            "events"
                        ),
                    "webhookByEvents":
                        current.get(
                            "webhookByEvents"
                        ),
                },
                flush=True,
            )

            repair_webhook(
                name,
                current,
            )

            print(
                "RFC_EVOLUTION_WEBHOOK_REPAIRED =",
                {
                    "instance": name,
                    "state": state,
                },
                flush=True,
            )

        except Exception as exc:
            failures += 1

            print(
                "RFC_EVOLUTION_WATCHDOG_ERROR =",
                {
                    "instance": name,
                    "state": state,
                    "error": repr(exc),
                },
                flush=True,
            )

    print(
        "RFC_EVOLUTION_WATCHDOG_DONE =",
        {
            "failures": failures,
        },
        flush=True,
    )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
