/**
 * uploadCSVToDrive.gs
 *
 * Scopo: carica/aggiorna un file CSV su Drive
 */

// Configurazione
const CFG = {
  INPUT_FILE_ID: "1mJ3sHcF5w7eXxV3kLc9sI4XN7afjfgGB",   // Il file da aggiornare
  OUTPUT_FILE_NAME: "filtered_clean.csv"      // Nome del file pulito
};

function uploadCSVToDrive() {
  // Trova il file di input tramite fileId
  const inputFile = DriveApp.getFileById(CFG.INPUT_FILE_ID);
  if (!inputFile) {
    Logger.log("File di input non trovato: " + CFG.INPUT_FILE_ID);
    return;
  }

  // Leggi contenuto
  let content = inputFile.getBlob().getDataAsString("UTF-8");

  // --- Pulizie opzionali se vuoi ---
  content = content.replace(/[\x00-\x1F]/g, "");
  content = content.replace(/""/g, "'");
  content = content.replace(/<[^>]+>/g, "");
  content = content.replace(/\r\n/g, "\n");

  // Cerca se esiste già il file di output
  const files = DriveApp.getFilesByName(CFG.OUTPUT_FILE_NAME);
  if (files.hasNext()) {
    const outFile = files.next();
    outFile.setContent(content);
    Logger.log("Aggiornato: " + CFG.OUTPUT_FILE_NAME);
  } else {
    DriveApp.createFile(CFG.OUTPUT_FILE_NAME, content, MimeType.PLAIN_TEXT);
    Logger.log("Creato nuovo file: " + CFG.OUTPUT_FILE_NAME);
  }
}
