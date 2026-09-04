import html
import os
import re
from pathlib import Path
from zipfile import ZipFile


def prepare_docx_for_gotenberg(ruta_docx: str) -> str:
    """
    Normaliza el DOCX para que LibreOffice/Gotenberg
    reproduzca la geometría aprobada tipo Aspose.

    NO modifica el DOCX original.
    """

    src = Path(ruta_docx)

    dst = src.with_name(
        src.stem + ".__gotenberg_aspose.docx"
    )

    with ZipFile(src, "r") as zin:
        items = zin.infolist()

        files = {
            item.filename: zin.read(item.filename)
            for item in items
        }

    # ------------------------------------------------------
    # 1. FUENTES
    # ------------------------------------------------------
    for name, data in list(files.items()):

        if not name.endswith(".xml"):
            continue

        xml = data.decode(
            "utf-8",
            errors="ignore",
        )

        xml = xml.replace(
            "Arial MT",
            "Arial",
        )

        xml = xml.replace(
            "ArialUnicodeMS",
            "Arial",
        )

        xml = xml.replace(
            "Arial Unicode MS",
            "Arial",
        )

        xml = xml.replace(
            "Lucida Sans Unicode",
            "Arial",
        )

        files[name] = xml.encode("utf-8")

    name = "word/document.xml"

    if name not in files:
        raise RuntimeError(
            "GOTENBERG_DOCUMENT_XML_MISSING"
        )

    xml = files[name].decode(
        "utf-8",
        errors="ignore",
    )

    # ------------------------------------------------------
    # 2. CÉDULA DE IDENTIFICACIÓN FISCAL
    #
    # Word guarda dos representaciones del textbox:
    # WPS + fallback VML.
    # ------------------------------------------------------
    title_a = "CÉDULA DE IDENTIFICACIÓN "
    title_b = "CÉDULA DE IDENTIFICACIÓN FISCAL"

    textbox_pattern = re.compile(
        r"<w:txbxContent>.*?</w:txbxContent>",
        re.S,
    )

    nuevo_textbox = (
        '<w:txbxContent>'
        '<w:p>'
        '<w:pPr>'
        '<w:spacing w:before="97" w:after="0"/>'
        '<w:ind w:left="0" w:right="0"/>'
        '<w:jc w:val="center"/>'
        '</w:pPr>'
        '<w:r>'
        '<w:rPr>'
        '<w:rFonts '
        'w:ascii="Arial" '
        'w:hAnsi="Arial" '
        'w:eastAsia="Arial" '
        'w:cs="Arial"/>'
        '<w:b/>'
        '<w:spacing w:val="-2"/>'
        '<w:sz w:val="20"/>'
        '<w:szCs w:val="20"/>'
        '</w:rPr>'
        '<w:t>'
        'CÉDULA DE IDENTIFICACIÓN FISCAL'
        '</w:t>'
        '</w:r>'
        '</w:p>'
        '</w:txbxContent>'
    )

    textbox_count = 0

    def fix_textbox(match):
        nonlocal textbox_count

        block = match.group(0)

        if (
            title_a not in block
            and title_b not in block
        ):
            return block

        textbox_count += 1
        return nuevo_textbox

    xml = textbox_pattern.sub(
        fix_textbox,
        xml,
    )

    if textbox_count != 2:
        raise RuntimeError(
            "GOTENBERG_CEDULA_TEXTBOX_COUNT:"
            f"{textbox_count}"
        )

    # ------------------------------------------------------
    # 3. ESPACIO CÉDULA -> TABLA IDENTIFICACIÓN
    #
    # Este es el GAP que aprobaste:
    # 125 twips = 6.25 pt.
    # ------------------------------------------------------
    tables = list(
        re.finditer(
            r"<w:tbl\b.*?</w:tbl>",
            xml,
            flags=re.S,
        )
    )

    table_start = None

    for match in tables:

        block = match.group(0)

        texts = re.findall(
            r"<w:t(?:\s[^>]*)?>(.*?)</w:t>",
            block,
            flags=re.S,
        )

        plain = " ".join(
            html.unescape(
                re.sub(
                    r"<[^>]+>",
                    "",
                    text,
                )
            ).strip()
            for text in texts
        )

        plain = re.sub(
            r"\s+",
            " ",
            plain,
        ).strip()

        if (
            "Datos de Identificación"
            in plain
            and "Contribuyente"
            in plain
        ):
            table_start = match.start()
            break

    if table_start is None:
        raise RuntimeError(
            "GOTENBERG_IDENTIFICATION_TABLE_NOT_FOUND"
        )

    spacer = (
        '<w:p>'
        '<w:pPr>'
        '<w:spacing '
        'w:before="0" '
        'w:after="0" '
        'w:line="125" '
        'w:lineRule="exact"/>'
        '</w:pPr>'
        '<w:r>'
        '<w:rPr>'
        '<w:sz w:val="2"/>'
        '<w:szCs w:val="2"/>'
        '</w:rPr>'
        '<w:t xml:space="preserve"> </w:t>'
        '</w:r>'
        '</w:p>'
    )

    xml = (
        xml[:table_start]
        + spacer
        + xml[table_start:]
    )

    files[name] = xml.encode("utf-8")

    with ZipFile(dst, "w") as zout:

        for item in items:
            zout.writestr(
                item,
                files[item.filename],
            )

    print(
        "[GOTENBERG DOCX ASPOSE NORMALIZED]",
        {
            "src": str(src),
            "dst": str(dst),
            "textboxes": textbox_count,
            "spacer_twips": 125,
        },
        flush=True,
    )

    return str(dst)


def normalize_pdf_to_aspose_layout(
    pdf_path: str,
) -> bool:
    """
    Corrección PDF exacta correspondiente al
    RFC_GOTENBERG_NORMALIZED_PROOF aprobado.

    Solo modifica geometría fija de página 1.
    Página 2 NO se toca.
    """

    import pymupdf as fitz

    tmp_path = (
        pdf_path
        + ".aspose_layout.tmp.pdf"
    )

    src_doc = None
    doc = None

    try:
        # Fuente inmutable para copiar el bloque derecho.
        src_doc = fitz.open(pdf_path)
        doc = fitz.open(pdf_path)

        if doc.page_count < 1:
            raise RuntimeError(
                "GOTENBERG_PDF_NO_PAGES"
            )

        page = doc[0]

        width = float(page.rect.width)
        height = float(page.rect.height)

        # Esta normalización corresponde a Carta.
        if (
            abs(width - 612.0) > 1.0
            or abs(height - 792.0) > 1.0
        ):
            raise RuntimeError(
                "GOTENBERG_UNEXPECTED_PAGE_SIZE:"
                f"{width}x{height}"
            )

        gray = (
            0.9450980392,
            0.9450980392,
            0.9450980392,
        )

        black = (0.0, 0.0, 0.0)
        white = (1.0, 1.0, 1.0)

        # --------------------------------------------------
        # A. RECUADRO CÉDULA
        #
        # Geometría verificada contra el PDF aprobado.
        # --------------------------------------------------
        page.draw_rect(
            fitz.Rect(
                37.07,
                121.069,
                289.97,
                122.750,
            ),
            color=None,
            fill=gray,
            overlay=True,
        )

        page.draw_rect(
            fitz.Rect(
                36.87,
                122.480,
                290.17,
                122.920,
            ),
            color=None,
            fill=gray,
            overlay=True,
        )

        page.draw_rect(
            fitz.Rect(
                36.77,
                142.169,
                290.27,
                144.050,
            ),
            color=None,
            fill=white,
            overlay=True,
        )

        page.draw_line(
            fitz.Point(
                37.00,
                142.050,
            ),
            fitz.Point(
                289.90,
                142.050,
            ),
            color=black,
            width=0.25,
            overlay=True,
        )

        page.draw_line(
            fitz.Point(
                37.07,
                121.069,
            ),
            fitz.Point(
                289.97,
                121.069,
            ),
            color=black,
            width=0.14,
            overlay=True,
        )

        page.draw_line(
            fitz.Point(
                37.07,
                121.069,
            ),
            fitz.Point(
                37.07,
                142.069,
            ),
            color=black,
            width=0.14,
            overlay=True,
        )

        page.draw_line(
            fitz.Point(
                289.97,
                121.069,
            ),
            fitz.Point(
                289.97,
                142.069,
            ),
            color=black,
            width=0.14,
            overlay=True,
        )

        # --------------------------------------------------
        # B. BLOQUE DERECHO
        #
        # CONSTANCIA + LUGAR/FECHA + BARCODE.
        # LibreOffice lo coloca 6.44501 pt arriba.
        # --------------------------------------------------
        page.draw_rect(
            fitz.Rect(
                305.50,
                158.40,
                574.00,
                303.60,
            ),
            color=None,
            fill=white,
            overlay=True,
        )

        clip = fitz.Rect(
            305.70,
            158.60,
            573.80,
            303.40,
        )

        shift_y = 6.44501

        destination = fitz.Rect(
            clip.x0,
            clip.y0 + shift_y,
            clip.x1,
            clip.y1 + shift_y,
        )

        page.show_pdf_page(
            destination,
            src_doc,
            0,
            clip=clip,
            keep_proportion=False,
            overlay=True,
        )

        doc.set_metadata({})

        doc.save(
            tmp_path,
            garbage=4,
            deflate=True,
        )

        doc.close()
        doc = None

        src_doc.close()
        src_doc = None

        os.replace(
            tmp_path,
            pdf_path,
        )

        print(
            "[GOTENBERG PDF ASPOSE NORMALIZED]",
            {
                "pdf": pdf_path,
                "page_size": (
                    width,
                    height,
                ),
                "right_shift_pt":
                    shift_y,
            },
            flush=True,
        )

        return True

    finally:

        try:
            if doc is not None:
                doc.close()
        except Exception:
            pass

        try:
            if src_doc is not None:
                src_doc.close()
        except Exception:
            pass

        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
