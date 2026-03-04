/**
 * uploadCSVToDrive.gs
 *
 * Scopo:
 * - Carica/aggiorna il CSV su Google Drive
 * - Pulisce caratteri invisibili, doppi apici e HTML
 */

const CFG = {
  INPUT_FILE_NAME: "filtered_clean.csv",   // il file generato da filter_feed.py
  OUTPUT_FILE_NAME: "filtered_clean.csv"   // stesso nome sul Drive
};

function uploadCSVToDrive() {
  // Recupera il file CSV locale sul container (se usi GitHub Actions con clasp, assicurati che sia copiato)
  const folder = DriveApp.getRootFolder(); // puoi cambiare se vuoi una cartella specifica
  const files = folder.getFilesByName(CFG.OUTPUT_FILE_NAME);

  // Leggi contenuto
  let content = "";
  if (files.hasNext()) {
    content = files.next().getBlob().getDataAsString("UTF-8");
  } else {
    Logger.log("File locale non trovato: " + CFG.INPUT_FILE_NAME);
    return;
  }

  // --- Pulizie ---
  content = content.replace(/[\x00-\x1F]/g, "");
  content = content.replace(/""/g, "'");
  content = content.replace(/<[^>]+>/g, "");
  content = content.replace(/\r\n/g, "\n");

  // Controlla se esiste già su Drive
  const driveFiles = DriveApp.getFilesByName(CFG.OUTPUT_FILE_NAME);
  if (driveFiles.hasNext()) {
    const outFile = driveFiles.next();
    outFile.setContent(content);
    Logger.log("Aggiornato su Drive: " + CFG.OUTPUT_FILE_NAME);
  } else {
    DriveApp.createFile(CFG.OUTPUT_FILE_NAME, content, MimeType.PLAIN_TEXT);
    Logger.log("Creato nuovo file su Drive: " + CFG.OUTPUT_FILE_NAME);
  }
}
