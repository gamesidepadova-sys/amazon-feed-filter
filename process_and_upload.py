from datetime import date
import csv
import requests
import io
import re
import os
import pandas as pd

# =========================================================
# CONFIG
# =========================================================

INPUT_URL = "http://listini.sellrapido.com/wh/_export_easytechonline_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"
DAILY_DIR = "daily_snapshots"

os.makedirs(DAILY_DIR, exist_ok=True)

# =========================================================
# FORNITORI
# =========================================================

ALLOWED_SUPPLIERS = {
    "0429",
    "0432",
    "0433",
    "0434",
    "0435",
}

# =========================================================
# CATEGORIE PRINCIPALI
# =========================================================

ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "consumabili e ufficio",
}

# =========================================================
# ESCLUSIONI TITOLO
# =========================================================

EXCLUDE_TITLE_SUBSTRINGS = {
    "phs-memory",
    "montatura",
    "blueoptics",
    "origin storage",
    "integral",
    "coreparts",
    "inline",
    "battery",
    "fellowes",
    "ricambio",
    "delock",
}

# =========================================================
# ESCLUSIONI MARCHE GLOBALI
# =========================================================

EXCLUDE_BRANDS = {
    "bluelan",
    "blueoptics",
    "cables direct",
    "dynamic alliances",
    "efb-elektronik",
    "it-budget",
    "microconnect",
    "nobo",
    "pdt",
    "one for all",
}

# =========================================================
# ESCLUSIONI CAT2
# =========================================================

EXCLUDE_CAT2_SUBSTRINGS = {
    "consumabili",
    "audio",
}

EXCLUDE_CAT2_EXACT = {
    "video digitale",
    "televisori",
    "home cinema",
    "bellezza e cura del corpo",
    "cura dei capelli",
    "tutto salute, beauty e fitness",
    "fitness technology",
    "ufficio",
    "scuola",
}

# =========================================================
# ESCLUSIONI CAT3
# =========================================================

EXCLUDE_CAT3_SUBSTRINGS = {
    "software",
    "stampanti",
    "alimentatori",
    "scanner",
    "borse e custodie",
    "cartucce",
}

# =========================================================
# CAVI E ACCESSORI:
# SOLO QUESTE MARCHE
# =========================================================

ALLOWED_BRANDS_CAT3 = {
    "cavi e accessori": {
        "adata",
        "asrock",
        "asus",
        "corsair",
        "crucial",
        "digitus",
        "fujitsu",
        "fujitsu technology solutions",
        "g.skill",
        "gigabyte",
        "hp",
        "hp enterprise",
        "hpe",
        "intel",
        "intellinet",
        "intenso",
        "kingston",
        "kingston technology",
        "lexar",
        "nvidia",
        "patriot memory",
        "samsung",
        "sandisk",
        "sapphire",
        "seagate",
        "sharkoon",
        "team group",
        "toshiba",
        "transcend",
        "ubiquiti",
        "verbatim",
        "viewsonic",
        "xiaomi",
        "zebra",
        "zyxel",
    },
}

MIN_QTY = 10
MAX_DIFF_0434 = 20

# =========================================================
# PESO / MARCATORE POLEEPO
# =========================================================

SUPPLIER_WEIGHT = {
    "0429": 99.29,
    "0432": 99.32,
    "0433": 99.33,
    "0434": 99.34,
    "0435": 99.35,
}

# =========================================================
# UTILS
# =========================================================

def today_tag(prefix):
    return f"{prefix}_{date.today().strftime('%Y%m%d')}"


def is_first_run_today():
    today = date.today().isoformat()
    expected = f"snapshot_{today}.csv"
    return expected not in os.listdir(DAILY_DIR)


def no_snapshot_exists_yet():
    return len(os.listdir(DAILY_DIR)) == 0


def save_daily_snapshot(df):
    today = date.today().isoformat()
    df.to_csv(
        f"{DAILY_DIR}/snapshot_{today}.csv",
        index=False
    )


# =========================================================
# CARICAMENTO SNAPSHOT PRECEDENTE
# =========================================================

def load_yesterday_snapshot():

    files = sorted(
        os.listdir(DAILY_DIR)
    )

    if not files:
        print(
            "⚠️ Nessuno snapshot precedente trovato. Nessun tag oggi."
        )
        return None

    path = f"{DAILY_DIR}/{files[-1]}"

    if os.path.getsize(path) == 0:
        print(
            f"⚠️ Snapshot vuoto ({path}). Nessun tag oggi."
        )
        return None

    try:
        return pd.read_csv(path)

    except pd.errors.EmptyDataError:
        print(
            f"⚠️ Snapshot non leggibile ({path}). Nessun tag oggi."
        )
        return None


# =========================================================
# CONVERSIONI
# =========================================================

def to_int(x, default=0):

    try:
        s = str(x or "").strip()

        if not s:
            return default

        s = s.replace(
            ".",
            ""
        ).replace(
            ",",
            "."
        )

        return int(float(s))

    except:
        return default


def to_float(x, default=0.0):

    try:
        s = str(x or "").strip()

        if not s:
            return default

        s = s.replace(
            ",",
            "."
        )

        return float(s)

    except:
        return default


def supplier_from_sku(sku: str):

    parts = (
        sku or ""
    ).strip().split("_")

    if len(parts) >= 3:
        return parts[1]

    return ""


def norm(s: str):

    return str(
        s or ""
    ).strip().lower()


def clean_text(text: str):

    t = str(
        text or ""
    )

    t = re.sub(
        "<.*?>",
        " ",
        t
    )

    t = t.replace(
        "&nbsp;",
        " "
    )

    t = t.replace(
        '"',
        ""
    )

    t = t.replace(
        "|",
        " "
    )

    t = t.replace(
        "\n",
        " "
    )

    t = t.replace(
        "\r",
        " "
    )

    t = re.sub(
        " +",
        " ",
        t
    )

    return t.strip()


def valid_ean(ean: str):

    e = (
        ean or ""
    ).strip()

    return (
        e.isdigit()
        and 8 <= len(e) <= 14
    )


# =========================================================
# TAG LOGIC
# =========================================================

def detect_new(today_df, yesterday_df):

    if yesterday_df is None:

        today_df["status"] = "UNCHANGED"

        return today_df

    yesterday_eans = set(
        yesterday_df["ean"]
    )

    today_df["status"] = today_df[
        "ean"
    ].apply(
        lambda e:
        "NEW"
        if e not in yesterday_eans
        else "UNCHANGED"
    )

    return today_df


def detect_stock_trend(
    today_df,
    yesterday_df
):

    if yesterday_df is None:

        today_df[
            "stock_trend"
        ] = "UNCHANGED"

        return today_df

    merged = today_df.merge(
        yesterday_df[
            [
                "ean",
                "quantita"
            ]
        ],
        on="ean",
        how="left",
        suffixes=(
            "",
            "_yesterday"
        )
    )

    def trend(row):

        if pd.isna(
            row["quantita_yesterday"]
        ):
            return "UNCHANGED"

        if (
            row["quantita"] > 14
            and
            row["quantita_yesterday"] <= 14
        ):
            return "RECOVERED"

        if (
            row["quantita"]
            >
            row["quantita_yesterday"]
        ):
            return "INCREASED"

        return "UNCHANGED"

    merged[
        "stock_trend"
    ] = merged.apply(
        trend,
        axis=1
    )

    return merged


def apply_tags(df):

    df["tag"] = ""

    new_tag = today_tag(
        "new"
    )

    mod_tag = today_tag(
        "mod"
    )

    for idx, row in df.iterrows():

        tags = []

        if row["status"] == "NEW":
            tags.append(
                new_tag
            )

        if row[
            "stock_trend"
        ] in (
            "RECOVERED",
            "INCREASED"
        ):
            tags.append(
                mod_tag
            )

        df.at[
            idx,
            "tag"
        ] = ",".join(tags)

    return df


# =========================================================
# MAIN
# =========================================================

def main():

    print(
        "📥 Scarico feed originale..."
    )

    resp = requests.get(
        INPUT_URL
    )

    resp.raise_for_status()

    # IDENTICO ALLO SCRIPT CHE FUNZIONAVA
    text = resp.content.decode(
        "utf-8-sig",
        errors="replace"
    )

    reader = csv.DictReader(
        io.StringIO(text),
        delimiter="|"
    )

    reader.fieldnames = [
        name.replace(
            "\ufeff",
            ""
        )
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

    rows_raw = list(
        reader
    )

    # =====================================================
    # RAGGRUPPAMENTO PER EAN
    # =====================================================

    ean_groups = {}

    for r in rows_raw:

        try:

            sku = r.get(
                "sku"
            ) or ""

            supplier = supplier_from_sku(
                sku
            )

            if supplier not in ALLOWED_SUPPLIERS:
                continue

            # -------------------------------------------------
            # CAT1
            # -------------------------------------------------

            cat1 = norm(
                r.get("cat1")
                or r.get("categoria")
                or ""
            )

            if cat1 not in ALLOWED_CAT1:
                continue

            # -------------------------------------------------
            # CAT2 / CAT3 / MARCA
            # -------------------------------------------------

            cat2 = norm(
                r.get("cat2")
                or ""
            )

            cat3 = norm(
                r.get("cat3")
                or ""
            )

            marca = norm(
                r.get("marca")
                or ""
            )

            # -------------------------------------------------
            # TITOLO
            # -------------------------------------------------

            titolo = norm(
                r.get(
                    "titolo_prodotto"
                )
                or r.get(
                    "nome"
                )
                or ""
            )

            # -------------------------------------------------
            # ESCLUSIONE MARCHE GLOBALI
            # -------------------------------------------------

            if marca in EXCLUDE_BRANDS:
                continue

            # -------------------------------------------------
            # ESCLUSIONI TITOLO
            # -------------------------------------------------

            if any(
                x in titolo
                for x in EXCLUDE_TITLE_SUBSTRINGS
            ):
                continue

            # -------------------------------------------------
            # ESCLUSIONI CAT2
            # -------------------------------------------------

            if any(
                x in cat2
                for x in EXCLUDE_CAT2_SUBSTRINGS
            ):
                continue

            if cat2 in EXCLUDE_CAT2_EXACT:
                continue

            # -------------------------------------------------
            # ESCLUSIONI CAT3
            # -------------------------------------------------

            if any(
                x in cat3
                for x in EXCLUDE_CAT3_SUBSTRINGS
            ):
                continue

            # -------------------------------------------------
            # CAVI E ACCESSORI
            # SOLO MARCHE AUTORIZZATE
            # -------------------------------------------------

            if cat3 in ALLOWED_BRANDS_CAT3:

                if marca not in ALLOWED_BRANDS_CAT3[
                    cat3
                ]:
                    continue

            # -------------------------------------------------
            # QUANTITA'
            # -------------------------------------------------

            qty = to_int(
                r.get("quantita")
                or r.get("qty")
            )

            if qty < MIN_QTY:
                continue

            # -------------------------------------------------
            # EAN
            # -------------------------------------------------

            ean = clean_text(
                r.get("ean")
                or ""
            )

            if not valid_ean(ean):
                continue

            # -------------------------------------------------
            # IMMAGINE
            # -------------------------------------------------

            image = (
                r.get(
                    "immagine_principale"
                )
                or ""
            ).strip()

            if not image.startswith(
                "https://"
            ):
                continue

            # -------------------------------------------------
            # PREZZO
            # -------------------------------------------------

            prezzo = to_float(
                r.get(
                    "prezzo_iva_esclusa"
                )
            )

            spedizione = to_float(
                r.get(
                    "costo_spedizione"
                )
            )

            prezzo_totale = (
                prezzo
                +
                spedizione
            )

            # -------------------------------------------------
            # RIGA
            # -------------------------------------------------

            row = {
                k: clean_text(
                    r.get(k)
                    or ""
                )
                for k in fields
            }

            row["quantita"] = qty

            row["_original_sku"] = sku

            row["_price"] = prezzo_totale

            row["_supplier"] = supplier

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
        # PREZZO MINIMO
        # -------------------------------------------------

        min_row = min(
            rows,
            key=lambda x:
            x["_price"]
        )

        min_price = min_row[
            "_price"
        ]

        # -------------------------------------------------
        # PRIORITA' 0434
        # -------------------------------------------------

        row_0434 = min(
            (
                r
                for r in rows
                if r["_supplier"] == "0434"
            ),
            key=lambda x:
            x["_price"],
            default=None
        )

        if (
            row_0434
            and
            row_0434["_price"]
            <= min_price + MAX_DIFF_0434
        ):
            best_row = row_0434

        else:
            best_row = min_row

        best_by_ean[
            ean
        ] = best_row

    # =====================================================
    # TAG + SNAPSHOT
    # =====================================================

    today_df = pd.DataFrame(
        best_by_ean.values()
    )

    yesterday_df = load_yesterday_snapshot()

    today_df = detect_new(
        today_df,
        yesterday_df
    )

    today_df = detect_stock_trend(
        today_df,
        yesterday_df
    )

    # -----------------------------------------------------
    # LOGICA DEFINITIVA
    # -----------------------------------------------------

    if no_snapshot_exists_yet():

        print(
            "🟡 Nessuno snapshot precedente → "
            "salvo snapshot base senza tag"
        )

        today_df["tag"] = ""

        save_daily_snapshot(
            today_df
        )

    elif is_first_run_today():

        print(
            "🟢 Primo run del giorno → "
            "assegno i tag"
        )

        today_df = apply_tags(
            today_df
        )

        save_daily_snapshot(
            today_df
        )

    else:

        print(
            "⚪ Run successivo → "
            "niente tag, niente snapshot"
        )

        today_df["tag"] = ""

    # =====================================================
    # SCRITTURA FILE FINALE
    # =====================================================

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8",
        newline=""
    ) as out:

        writer = csv.DictWriter(
            out,
            fieldnames=fields + ["tag"],
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            escapechar="\\"
        )

        writer.writeheader()

        for _, r in today_df.iterrows():

            supplier_best = r[
                "_supplier"
            ]

            peso_val = SUPPLIER_WEIGHT.get(
                supplier_best
            )

            if peso_val is not None:

                r["peso"] = (
                    "24"
                    +
                    str(
                        int(
                            round(
                                float(
                                    peso_val
                                )
                                * 100
                            )
                        )
                    )
                )

            else:

                r["peso"] = "24"

            r = r.to_dict()

            # -------------------------------------------------
            # RIMOZIONE COLONNE TECNICHE
            # -------------------------------------------------

            for col in [
                "_price",
                "_supplier",
                "_original_sku",
                "status",
                "stock_trend"
            ]:

                r.pop(
                    col,
                    None
                )

            writer.writerow(
                r
            )

    print(
        f"\n📝 Feed generato: {OUTPUT_FILE}"
    )


# =========================================================
# AVVIO
# =========================================================

if __name__ == "__main__":
    main()
