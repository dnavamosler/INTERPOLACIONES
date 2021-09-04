import CSVToJSON from "csvtojson";
import moment from "moment";
moment.locale("es");


const obtenerDatos = async (path) => {
    const datos = await CSVToJSON({ delimiter: ";" }).fromFile(path);

    return datos;
};

const main = async () => {
    const rutas = ["DB/1.csv"];
    const dataExpediciones = await obtenerDatos(rutas);
    // const expedicionesValidas = dataExpediciones.filter((item) => item.INVALIDA === 0);
    // console.table(expedicionesValidas);
}

export { obtenerDatos };