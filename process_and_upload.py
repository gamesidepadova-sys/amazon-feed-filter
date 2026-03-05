INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "filtered_clean.csv"  # CSV filtrato da fornire a Poleepo

ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0383"}
ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "clima e brico",
    "consumabili e ufficio",
    "salute, beauty e fitness",
}
MIN_QTY = 10

# ----------------------
# Funzioni di supporto
# ----------------------
def supplier_from_sku(sku):
    parts = (sku or "").split("_")
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""

def norm(s):
    return str(s or "").strip().lower()

# ----------------------
# Script principale
# ----------------------
with open(INPUT_FILE, "r", encoding="utf-8-sig") as fin, \
     open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline='') as fout:

    header = fin.readline()
    fout.write(header)  # Scrive tutte le colonne originali

    for line in fin:
        parts = line.rstrip("\n").split("|")
        if len(parts) < 5:
            continue  # riga malformata

        sku = parts[1].strip()
        cat1 = norm(parts[0])
        try:
            qty = int(parts[4])
        except:
            qty = 0

        # Applica filtri
        if not sku or supplier_from_sku(sku) not in ALLOWED_SUPPLIERS:
            continue
        if cat1 not in ALLOWED_CAT1:
            continue
        if qty < MIN_QTY:
            continue

        fout.write(line)  # Scrive la riga intera così com’è
