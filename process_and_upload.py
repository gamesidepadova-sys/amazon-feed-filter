from datetime import date
import csv
import requests
import io
import re
import os
import pandas as pd
import hashlib

# =========================================================
# CONFIG
# =========================================================

INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"

ALLOWED_SUPPLIERS = {"0372", "0373", "0393", "0382", "0383"}

ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "consumabili e ufficio",
    "salute, beauty e fitness",
}

EXCLUDE_TITLE_SUBSTRINGS = {
    "phs-memory",
    "montatura",
    "blueoptics",
    "origin storage",
    "integral"
}

MIN_QTY = 10
MAX_DIFF_0373 = 20

SUPPLIER_WEIGHT = {
    "0372": 99.72,
    "0373": 99.73,
    "0374": 99.74,
    "0380": 99.80,
    "0393": 99.93,
    "0382": 99.82,
    "0383": 99.83
}

# =========================================================
# UTILS
# =========================================================

def to_int(x, default=0):
    try:
        s = str(x or "").strip()
        if not s:
            return default
        s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except:
        return default


def to_float(x, default=0.0):
    try:
        s = str(x or "").strip()
        if not s:
            return default
        s = s.replace(",", ".")
        return float(s)
    except:
        return default


def supplier_from_sku(sku: str) -> str:
    parts = (sku or "").strip().split("_")
    if len(parts) >= 3:
        return parts[1]
    return ""


def norm(s: str) -> str:
    return str(s or "").strip().lower()


def clean_text(text: str) -> str:
    t = str(text or "")
    t = re.sub("<.*?>", " ", t)
    t = t.replace("&nbsp;", " ")
    t = t.replace('"', "")
    t = t.replace("|", " ")
    t = t.replace("\n", " ")
    t = t.replace("\r", " ")
    t = re.sub(" +", " ", t)
    return t.strip()


def valid_ean(ean: str) -> bool:
    e = (ean or "").strip()
    return e.isdigit() and 8 <= len(e) <= 14


# =========================================================
# MAIN
# =========================================================

def main():

    print("📥 Scarico feed originale...")

    resp = requests.get(INPUT_URL)
    resp.raise_for_status()

    text = resp.content.decode(
        "utf-8-sig",
        errors="replace"
    )

    reader = csv.DictReader(
        io.StringIO(text),
        delimiter="|"
    )

    reader.fieldnames = [
        name.replace("\ufeff", "")
        for name in reader.fieldnames
    ]

    fields = [
        "cat1",
        "sku",
        "ean",
        "mpn",
        "quantita",
        "prezzo_iva_esclusa",
        "titolo_prodotto",
        "immagine_principale",
        "descrizione_prodotto",
        "costo_spedizione",
        "cat2",
        "cat3",
        "marca",
        "peso"
    ]

    rows_raw = list(reader)

    # =====================================================
    # RAGGRUPPAMENTO PER EAN
    # =====================================================

    ean_groups = {}

    for r in rows_raw:

        try:

            sku = r.get("sku") or ""

            supplier = supplier_from_sku(sku)

            if supplier not in ALLOWED_SUPPLIERS:
                continue

            cat1 = norm(
                r.get("cat1")
                or r.get("categoria")
                or ""
            )

            if cat1 not in ALLOWED_CAT1:
                continue

            titolo = norm(
                r.get("titolo_prodotto")
                or r.get("nome")
                or ""
            )

            if any(
                x in titolo
                for x in EXCLUDE_TITLE_SUBSTRINGS
            ):
                continue

            # -------------------------------------------------
            # GIACENZA DEL SINGOLO FORNITORE
            #
            # Se questo fornitore ha meno di 10 pezzi,
            # viene esclusa SOLO QUESTA RIGA.
            #
            # Se esiste un altro fornitore con lo stesso EAN
            # e >= 10 pezzi, quell'altro rimane candidato.
            # -------------------------------------------------

            qty = to_int(
                r.get("quantita")
                or r.get("qty")
            )

            if qty < MIN_QTY:
                continue

            ean = clean_text(
                r.get("ean") or ""
            )

            if not valid_ean(ean):
                continue

            # -------------------------------------------------
            # IMMAGINE
            # -------------------------------------------------

            immagine = (
                r.get("immagine_principale")
                or ""
            ).strip()

            if not immagine.startswith("https://"):
                continue

            # -------------------------------------------------
            # PREZZO
            # -------------------------------------------------

            prezzo = to_float(
                r.get("prezzo_iva_esclusa")
            )

            spedizione = to_float(
                r.get("costo_spedizione")
            )

            # Prezzo utilizzato per confrontare i fornitori
            prezzo_totale = prezzo + spedizione

            # -------------------------------------------------
            # RIGA ORIGINALE
            # -------------------------------------------------

            row = {
                k: clean_text(
                    r.get(k) or ""
                )
                for k in fields
            }

            # Manteniamo la quantità originale
            row["quantita"] = str(qty)

            # Manteniamo il prezzo originale
            row["prezzo_iva_esclusa"] = clean_text(
                r.get("prezzo_iva_esclusa") or ""
            )

            # Dati interni
            row["_original_sku"] = sku
            row["_supplier"] = supplier
            row["_qty"] = qty
            row["_price"] = prezzo_totale

            ean_groups.setdefault(
                ean,
                []
            ).append(row)

        except:
            continue

    # =====================================================
    # SCELTA MIGLIORE PER EAN
    # =====================================================

    best_by_ean = {}

    for ean, rows in ean_groups.items():

        if not rows:
            continue

        # -------------------------------------------------
        # A questo punto tutte le righe hanno già
        # quantità >= MIN_QTY.
        # -------------------------------------------------

        available_rows = [
            r for r in rows
            if r["_qty"] >= MIN_QTY
        ]

        if not available_rows:
            continue

        # -------------------------------------------------
        # PREZZO TOTALE PIÙ BASSO
        # -------------------------------------------------

        min_row = min(
            available_rows,
            key=lambda x: x["_price"]
        )

        min_price = min_row["_price"]

        # -------------------------------------------------
        # CERCA 0373 TRA I FORNITORI DISPONIBILI
        # -------------------------------------------------

        rows_0373 = [
            r for r in available_rows
            if r["_supplier"] == "0373"
        ]

        if rows_0373:

            # Se ci fossero più righe 0373 per lo stesso EAN,
            # prendiamo quella col prezzo totale più basso.
            row_0373 = min(
                rows_0373,
                key=lambda x: x["_price"]
            )

            # 0373 viene preferito se il suo prezzo totale
            # non supera di più di 20 € il prezzo migliore.
            if row_0373["_price"] <= (
                min_price + MAX_DIFF_0373
            ):
                best_row = row_0373
            else:
                best_row = min_row

        else:

            best_row = min_row

        # -------------------------------------------------
        # CONTROLLO DI SICUREZZA
        # -------------------------------------------------

        if best_row["_qty"] < MIN_QTY:
            continue

        best_by_ean[ean] = best_row

    # =====================================================
    # GENERAZIONE FILE FINALE
    # =====================================================

    today_df = pd.DataFrame(
        best_by_ean.values()
    )

    rows_final = []

    for _, r in today_df.iterrows():

        supplier_best = r["_supplier"]

        peso_val = SUPPLIER_WEIGHT.get(
            supplier_best
        )

        if peso_val is not None:

            r["peso"] = (
                "24"
                + str(
                    int(
                        round(
                            float(peso_val) * 100
                        )
                    )
                )
            )

        else:

            r["peso"] = "24"

        r = r.to_dict()

        # -------------------------------------------------
        # RIMUOVI COLONNE TECNICHE
        # -------------------------------------------------

        for col in [
            "_price",
            "_supplier",
            "_original_sku",
            "_qty"
        ]:
            r.pop(col, None)

        rows_final.append(r)

    # =====================================================
    # ORDINA PER EAN
    # =====================================================

    rows_final_sorted = sorted(
        rows_final,
        key=lambda x: x.get("ean", "")
    )

    # =====================================================
    # COSTRUZIONE CSV
    # =====================================================

    output_csv = []

    output_csv.append(
        "|".join(fields)
    )

    for r in rows_final_sorted:

        line = "|".join(
            str(r.get(f, ""))
            for f in fields
        )

        output_csv.append(line)

    final_bytes = (
        "\n".join(output_csv)
        .encode("utf-8")
    )

    # =====================================================
    # CONTROLLO HASH
    # =====================================================

    if os.path.exists(OUTPUT_FILE):

        with open(
            OUTPUT_FILE,
            "rb"
        ) as f:
            old_bytes = f.read()

        if (
            hashlib.md5(old_bytes).hexdigest()
            ==
            hashlib.md5(final_bytes).hexdigest()
        ):
            print(
                "⏭ Nessun cambiamento reale → skip"
            )
            return

    # =====================================================
    # SCRITTURA FILE
    # =====================================================

    with open(
        OUTPUT_FILE,
        "wb"
    ) as f:
        f.write(final_bytes)

    print(
        f"📝 Feed aggiornato: {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
