#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import difflib
import py_compile
import re
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path.cwd()
WEBHOOK = ROOT / "app" / "rfc_webhook.py"
WORKER = ROOT / "worker_jobs.py"

class PatchError(RuntimeError):
    pass

def rx(text, pattern, repl, label, count=1):
    new, n = re.subn(pattern, lambda _m: repl, text, count=count, flags=re.S)
    if n != count:
        raise PatchError(f"{label}: esperaba {count}, encontré {n}")
    return new

def compile_text(path, text):
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / path.name
        p.write_text(text, encoding="utf-8")
        py_compile.compile(str(p), doraise=True)

def patch_webhook(text):
    # Balance verificable
    text = rx(text,
        r'''client_message = \(\s*f"⚠️ \{requester_label\}, "\s*"este grupo no tiene una bolsa RFC "\s*"activa\."\s*\)''',
        '''client_message = (
                        "⚠️ RFC verificable no disponible\\n"
                        f"👤 {requester_label}\\n\\n"
                        "Este grupo no tiene una bolsa RFC activa."
                    )''',
        "webhook bolsa RFC")

    text = rx(text,
        r'''client_message = \(\s*f"⚠️ \{requester_label\}, "\s*"este grupo no tiene RFC "\s*"verificables asignados\."\s*\)''',
        '''client_message = (
                        "⚠️ RFC verificable no disponible\\n"
                        f"👤 {requester_label}\\n\\n"
                        "Este grupo no tiene RFC verificables asignados."
                    )''',
        "webhook verificables asignados")

    text = rx(text,
        r'''client_message = \(\s*f"⚠️ \{requester_label\}, "\s*"este grupo ya no tiene RFC "\s*"verificables disponibles\."\s*\)''',
        '''client_message = (
                        "⚠️ RFC verificable no disponible\\n"
                        f"👤 {requester_label}\\n\\n"
                        "Este grupo ya no tiene RFC verificables disponibles."
                    )''',
        "webhook limite grupo")

    text = rx(text,
        r'''client_message = \(\s*f"⚠️ \{requester_label\}, "\s*"este grupo alcanzó su límite "\s*"de RFC verificables dentro de "\s*"la bolsa compartida\."\s*\)''',
        '''client_message = (
                        "⚠️ RFC verificable no disponible\\n"
                        f"👤 {requester_label}\\n\\n"
                        "Este grupo alcanzó su límite de RFC verificables."
                    )''',
        "webhook limite compartido")

    text = rx(text,
        r'''client_message = \(\s*f"⚠️ \{requester_label\}, "\s*"no fue posible validar el saldo "\s*"de RFC verificables\. "\s*"Intenta nuevamente\."\s*\)''',
        '''client_message = (
                        "⚠️ No pudimos validar la disponibilidad\\n"
                        f"👤 {requester_label}\\n\\n"
                        "No fue posible validar la disponibilidad de RFC verificables.\\n"
                        "Intenta nuevamente en unos momentos."
                    )''',
        "webhook saldo verificable")

    # CURP verificable
    text = rx(text,
        r'''client_message = \(\s*f"⚠️ \{requester_label\}, "\s*"la CURP "\s*f"\{original_identifier\} "\s*"no fue localizada en RENAPO\.\\n\\n"\s*"Verifica que esté escrita "\s*"correctamente\."\s*\)''',
        '''client_message = (
                            "⚠️ CURP no localizada\\n"
                            f"👤 {requester_label}\\n"
                            f"🪪 CURP: {original_identifier}\\n\\n"
                            "No fue localizada en RENAPO.\\n"
                            "Verifica que esté escrita correctamente."
                        )''',
        "webhook CURP no localizada")

    text = rx(text,
        r'''client_message = \(\s*f"⚠️ \{requester_label\}, "\s*"no fue posible consultar la "\s*"CURP en este momento\.\\n\\n"\s*"El servicio de validación "\s*"de CURP está temporalmente "\s*"no disponible\. "\s*"Intenta nuevamente más tarde\."\s*\)''',
        '''client_message = (
                            "⚠️ Consulta temporalmente no disponible\\n"
                            f"👤 {requester_label}\\n"
                            f"🪪 CURP: {original_identifier}\\n\\n"
                            "No fue posible consultar la CURP en este momento.\\n"
                            "Intenta nuevamente más tarde."
                        )''',
        "webhook CURP servicio")

    text = rx(text,
        r'''client_message = \(\s*f"⚠️ \{requester_label\}, "\s*"la CURP fue localizada, pero "\s*"no fue posible convertirla "\s*"a RFC verificable\. "\s*"Intenta nuevamente\."\s*\)''',
        '''client_message = (
                            "⚠️ No pudimos completar la solicitud\\n"
                            f"👤 {requester_label}\\n"
                            f"🪪 CURP: {original_identifier}\\n\\n"
                            "La CURP fue localizada, pero no fue posible continuar "
                            "con la solicitud verificable.\\n"
                            "Intenta nuevamente."
                        )''',
        "webhook CURP conversion")

    # Pending / proveedor
    text = rx(text,
        r'''\(\s*f"⚠️ \{requester_label\}, "\s*"no fue posible registrar la "\s*"solicitud verificable\. "\s*"Intenta nuevamente\."\s*\),''',
        '''_client_status_message(
                            title="⚠️ No pudimos registrar la solicitud",
                            requester_label=requester_label,
                            query_type=original_query_type,
                            identifier=original_identifier,
                            body=(
                                "Ocurrió una interrupción temporal.\\n"
                                "Intenta nuevamente."
                            ),
                        ),''',
        "webhook pending save")

    text = rx(text,
        r'''\(\s*f"⚠️ \{requester_label\}, "\s*"no fue posible enviar la "\s*"solicitud al proveedor de "\s*"RFC verificables\."\s*\),''',
        '''_client_status_message(
                            title="⚠️ No pudimos iniciar la solicitud",
                            requester_label=requester_label,
                            query_type=original_query_type,
                            identifier=original_identifier,
                            body=(
                                "No fue posible iniciar el procesamiento.\\n"
                                "Intenta nuevamente."
                            ),
                        ),''',
        "webhook provider send")

    # Duplicado normal
    text = rx(text,
        r'''\(\s*f"⏳ \{requester_label\}, esta solicitud "\s*"ya está siendo procesada\.\\n"\s*"No es necesario volver a enviarla\."\s*\),''',
        '''_client_status_message(
                            title="⏳ Solicitud en proceso",
                            requester_label=requester_label,
                            query_type=(parsed.get("type") or ""),
                            identifier=(
                                CURP_RE.search(normalized_query).group(0).upper()
                                if (
                                    parsed.get("type") == "CURP"
                                    and CURP_RE.search(normalized_query)
                                )
                                else (
                                    RFC_RE.search(normalized_query).group(0).upper()
                                    if (
                                        parsed.get("type") == "RFC_ONLY"
                                        and RFC_RE.search(normalized_query)
                                    )
                                    else ""
                                )
                            ),
                            rfc=(
                                RFC_RE.search(normalized_query).group(0).upper()
                                if (
                                    parsed.get("type") == "RFC_IDCIF"
                                    and RFC_RE.search(normalized_query)
                                )
                                else ""
                            ),
                            idcif=(
                                IDCIF_RE.search(normalized_query).group(0)
                                if (
                                    parsed.get("type") == "RFC_IDCIF"
                                    and IDCIF_RE.search(normalized_query)
                                )
                                else ""
                            ),
                            body=(
                                "Esta solicitud ya está siendo procesada.\\n"
                                "No es necesario enviarla nuevamente."
                            ),
                        ),''',
        "webhook duplicado normal")

    return text

def patch_worker(text):
    # Timeout verificable
    text = rx(text,
        r'''if original_type == "CURP":.*?try:\s*if client_group_jid:\s*evolution_send_text_to_group\(\s*client_group_jid,\s*\(\s*f"⚠️ \{requester_label\}, "\s*"la solicitud verificable no recibió "\s*"respuesta dentro de 24 horas para "\s*f"\{identifier_text\}\.\\n\\n"\s*"Ya puedes volver a solicitarla\."\s*\),\s*instance_name=client_instance,\s*\)''',
        '''timeout_job_data = {
        "is_verifiable": True,
        "verifiable_original_type": original_type,
        "verifiable_original_identifier": original_identifier,
        "requester_label": requester_label,
        "batch_index": int(pending.get("batch_index") or 1),
        "batch_total": int(pending.get("batch_total") or 1),
    }

    try:
        if client_group_jid:
            evolution_send_text_to_group(
                client_group_jid,
                _job_client_message(
                    timeout_job_data,
                    title="⏱️ RFC verificable sin respuesta",
                    requester_label=requester_label,
                    body=(
                        "No recibimos respuesta dentro del tiempo permitido.\\n"
                        "Ya puedes solicitarlo nuevamente."
                    ),
                ),
                instance_name=client_instance,
            )''',
        "worker timeout verificable")

    # Fallbacks adjunto
    replacements = [
        (
            r'''\(\s*f"RFC: \{fallback_rfc\}\\n"\s*f"IDCIF: \{fallback_idcif\}\\n\\n"\s*"⚠️ La constancia fue generada, "\s*"pero hubo un problema temporal "\s*"al adjuntar el PDF\."\s*\),''',
            '''_job_client_message(
                                    job_data,
                                    title="⚠️ Problema al adjuntar la constancia",
                                    requester_label=requester_label,
                                    body=(
                                        "La constancia fue generada, pero hubo un problema "
                                        "temporal al adjuntar el PDF."
                                    ),
                                    result_mode=True,
                                    result_rfc=fallback_rfc,
                                    result_idcif=fallback_idcif,
                                ),''',
            "worker fallback ZIP verificable",
        ),
        (
            r'''\(\s*f"⚠️ \{requester_label\} "\s*"el lote se generó, "\s*"pero no pude adjuntarlo\.\\n"\s*f"\{zip_url\}"\s*\),''',
            '''_job_client_message(
                        job_data,
                        title="⚠️ Problema al adjuntar el lote",
                        requester_label=requester_label,
                        body=(
                            "El lote fue generado correctamente, pero no pudo adjuntarse.\\n"
                            "Puedes abrirlo desde el siguiente enlace:\\n"
                            f"{zip_url}"
                        ),
                        include_identity=False,
                    ),''',
            "worker ZIP normal",
        ),
        (
            r'''\(\s*f"⚠️ \{requester_label\} "\s*f"no pude adjuntar "\s*f"\{file_name\}\.\\n\{pdf_url\}"\s*\),''',
            '''(
                                "⚠️ Problema al adjuntar el documento\\n"
                                f"👤 {requester_label}\\n"
                                f"🧾 RFC: {rfc or 'N/D'}"
                                + (f"\\n🔢 IDCIF: {idcif}" if idcif else "")
                                + "\\n\\nEl documento fue generado, pero no pudo adjuntarse.\\n"
                                + f"{pdf_url}"
                            ),''',
            "worker batch item",
        ),
        (
            r'''\(\s*f"RFC: \{fallback_rfc\}\\n"\s*f"IDCIF: \{fallback_idcif\}\\n\\n"\s*"⚠️ No fue posible adjuntar "\s*"la constancia en este momento\."\s*\),''',
            '''_job_client_message(
                                job_data,
                                title="⚠️ Problema al adjuntar la constancia",
                                requester_label=requester_label,
                                body=(
                                    "La constancia fue generada, pero hubo un problema "
                                    "temporal al adjuntar el PDF."
                                ),
                                result_mode=True,
                                result_rfc=fallback_rfc,
                                result_idcif=fallback_idcif,
                            ),''',
            "worker fallback PDF 1",
        ),
        (
            r'''\(\s*f"RFC: \{fallback_rfc\}\\n"\s*f"IDCIF: \{fallback_idcif\}\\n\\n"\s*"⚠️ No fue posible adjuntar "\s*"la constancia\."\s*\),''',
            '''_job_client_message(
                                job_data,
                                title="⚠️ Problema al adjuntar la constancia",
                                requester_label=requester_label,
                                body=(
                                    "La constancia fue generada, pero hubo un problema "
                                    "temporal al adjuntar el PDF."
                                ),
                                result_mode=True,
                                result_rfc=fallback_rfc,
                                result_idcif=fallback_idcif,
                            ),''',
            "worker fallback PDF 2",
        ),
        (
            r'''\(\s*f"⚠️ \{requester_label\} "\s*"el documento se generó, "\s*"pero no pude adjuntarlo\.\\n"\s*f"\{pdf_url\}"\s*\),''',
            '''_job_client_message(
                    job_data,
                    title="⚠️ Problema al entregar el documento",
                    requester_label=requester_label,
                    body=(
                        "El documento fue generado, pero no pudo adjuntarse.\\n"
                        "Puedes abrirlo desde el siguiente enlace:\\n"
                        f"{pdf_url}"
                    ),
                ),''',
            "worker PDF normal",
        ),
    ]
    for pattern, repl, label in replacements:
        text = rx(text, pattern, repl, label)

    # RFC estados
    states = [
        ("cancelado", "cancelado"),
        ("suspendido", "suspendido"),
        ("no activo", "no activo"),
    ]
    for phrase, title in states:
        text = rx(text,
            rf'''\(\s*f"⚠️ \{{requester_label\}} "\s*"el RFC aparece como {re.escape(phrase)} "\s*"en la consulta oficial\.\\n\\n"\s*"No se generó la constancia\."\s*\),''',
            f'''_job_client_message(
                        job_data,
                        title="⚠️ RFC {title}",
                        requester_label=requester_label,
                        body=(
                            "El RFC aparece como {phrase} en la consulta oficial.\\n"
                            "No se generó la constancia."
                        ),
                    ),''',
            f"worker RFC {title}")

    # Comercial
    commercial = [
        (
            r'''f"⚠️ \{requester_label\} este bot ya no tiene RFC CLON disponibles\.",''',
            '''(
                            "⚠️ RFC CLON no disponible\\n"
                            f"👤 {requester_label}\\n\\n"
                            "Se alcanzó el límite disponible de RFC CLON."
                        ),''',
            "limite CLON",
        ),
        (
            r'''f"⚠️ \{requester_label\} este grupo ya no tiene RFC IDCIF disponibles\.",''',
            '''(
                                "⚠️ RFC IDCIF no disponible\\n"
                                f"👤 {requester_label}\\n\\n"
                                "Este grupo ya no tiene RFC IDCIF disponibles."
                            ),''',
            "grupo IDCIF",
        ),
        (
            r'''f"⚠️ \{requester_label\} este bot ya no tiene RFC IDCIF disponibles\.",''',
            '''(
                            "⚠️ RFC IDCIF no disponible\\n"
                            f"👤 {requester_label}\\n\\n"
                            "Se alcanzó el límite disponible de RFC IDCIF."
                        ),''',
            "limite IDCIF",
        ),
        (
            r'''\(\s*f"⚠️ \{requester_label\} "\s*"RFC verificable fue desactivado "\s*"para este bot\."\s*\),''',
            '''(
                            "⚠️ RFC verificable no disponible\\n"
                            f"👤 {requester_label}\\n\\n"
                            "El servicio RFC verificable no está activo actualmente."
                        ),''',
            "verificable desactivado worker",
        ),
        (
            r'''\(\s*f"⚠️ \{requester_label\} "\s*"este bot ya no tiene RFC "\s*"verificables disponibles\."\s*\),''',
            '''(
                            "⚠️ RFC verificable no disponible\\n"
                            f"👤 {requester_label}\\n\\n"
                            "No hay RFC verificables disponibles en este momento."
                        ),''',
            "limite verificable bot",
        ),
        (
            r'''\(\s*f"⚠️ \{requester_label\} "\s*"este grupo no tiene una bolsa "\s*"de RFC verificables asignada\."\s*\),''',
            '''(
                                "⚠️ RFC verificable no disponible\\n"
                                f"👤 {requester_label}\\n\\n"
                                "Este grupo no tiene RFC verificables asignados."
                            ),''',
            "verificable sin bolsa",
        ),
        (
            r'''\(\s*f"⚠️ \{requester_label\} "\s*"este grupo ya no tiene "\s*"RFC verificables disponibles\."\s*\),''',
            '''(
                                "⚠️ RFC verificable no disponible\\n"
                                f"👤 {requester_label}\\n\\n"
                                "Este grupo ya no tiene RFC verificables disponibles."
                            ),''',
            "verificable limite grupo",
        ),
        (
            r'''\(\s*f"⚠️ \{requester_label\} "\s*"este grupo alcanzó su "\s*"límite de RFC verificables "\s*"dentro de la bolsa compartida\."\s*\),''',
            '''(
                                "⚠️ RFC verificable no disponible\\n"
                                f"👤 {requester_label}\\n\\n"
                                "Este grupo alcanzó su límite de RFC verificables."
                            ),''',
            "verificable compartido worker",
        ),
    ]
    for pattern, repl, label in commercial:
        text = rx(text, pattern, repl, label)

    return text

def produce():
    if not WEBHOOK.exists() or not WORKER.exists():
        raise PatchError("No encuentro app/rfc_webhook.py o worker_jobs.py")

    ow = WEBHOOK.read_text(encoding="utf-8")
    ok = WORKER.read_text(encoding="utf-8")
    nw = patch_webhook(ow)
    nk = patch_worker(ok)
    compile_text(WEBHOOK, nw)
    compile_text(WORKER, nk)
    return ow, nw, ok, nk

def show_diff(path, old, new):
    print("\n".join(difflib.unified_diff(
        old.splitlines(), new.splitlines(),
        fromfile=str(path),
        tofile=str(path) + " (pass2)",
        lineterm=""
    )))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--check", action="store_true")
    p.add_argument("--diff", action="store_true")
    p.add_argument("--apply", action="store_true")
    a = p.parse_args()

    if sum([a.check, a.diff, a.apply]) != 1:
        p.error("Usa exactamente uno: --check, --diff o --apply")

    try:
        ow, nw, ok, nk = produce()
    except Exception as exc:
        print(f"❌ PASS2 ABORTADO: {exc}", file=sys.stderr)
        return 2

    if a.check:
        print("✅ PASS2: todas las coincidencias esperadas fueron encontradas")
        print("✅ PASS2: app/rfc_webhook.py compila")
        print("✅ PASS2: worker_jobs.py compila")
        print("✅ PASS2: no se escribió ningún archivo")
        return 0

    if a.diff:
        show_diff(WEBHOOK, ow, nw)
        show_diff(WORKER, ok, nk)
        return 0

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    bdir = Path("/root/rfc-message-backups")
    bdir.mkdir(parents=True, exist_ok=True)
    bw = bdir / f"rfc_webhook.py.pass2-{stamp}.bak"
    bk = bdir / f"worker_jobs.py.pass2-{stamp}.bak"
    shutil.copy2(WEBHOOK, bw)
    shutil.copy2(WORKER, bk)

    try:
        WEBHOOK.write_text(nw, encoding="utf-8")
        WORKER.write_text(nk, encoding="utf-8")
        py_compile.compile(str(WEBHOOK), doraise=True)
        py_compile.compile(str(WORKER), doraise=True)
    except Exception as exc:
        shutil.copy2(bw, WEBHOOK)
        shutil.copy2(bk, WORKER)
        print(f"❌ PASS2 falló y se restauraron backups: {exc}", file=sys.stderr)
        return 3

    print("✅ PASS2 APLICADO Y COMPILADO")
    print(f"Backup webhook: {bw}")
    print(f"Backup worker:  {bk}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
