const csv = require('csv-parser');

const SEPARADOR = ';';
/*
const obtenerDatos = async (path) => {
    const datos = await CSVToJSON({ delimiter: SEPARADOR }).fromFile(path);
    return datos;
};
*/
const LEER_PC = async (URL) => {
    const OPTIONS = csv({encoding: 'utf8', separator:SEPARADOR});
    const stream = await fs.createReadStream(URL).pipe(OPTIONS);
    
    stream.once('data',  () => { console.log('INICIANDO STREAM...\n'); });
    stream.on('data', (chunk) => {
        LISTADO_PC.push(chunk);
    });
    stream.on('end', () => {
        console.log('FIN DEL  STREAM...\n');
    });
}

//export { LEER_PC };