const { createObjectCsvWriter: createCsvWriter } = require("csv-writer");

const crearArchivos = async ({ records, headers, pathResult }) => {
  const fileWrite = createCsvWriter({
    path: pathResult,
    encoding: "utf8",
    fieldDelimiter: ";",
    header: headers,
  });

  await fileWrite.writeRecords(records);
};

module.exports = crearArchivos;
