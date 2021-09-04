const geolib = require("geolib");
// const {createObjectCsvWriter} = require("csv-writer");

const csv = require("csv-parser");
const fs = require("fs");

const moment = require("moment");
moment.locale("es");

// const auxiliar = require('./AUXILIARES/manejadorArrays');
const SEPARADOR = ";";
const FORM_FECH = "YYYY/MM/DD HH:mm:ss";
//const PUNTOS_SERVICIO = [];

const URL_TRACKING = "./ARCHIVOS/TRACKING/TEST.csv";
const URL_PC = "./ARCHIVOS/PC.csv";
//const URL_EXPEDICIONES = './SALIDAS/expediciones.csv';
const LISTADO_PC = [];
const LISTADO_TRACKING = [];

// const LISTADO_PASADAS = [];
const MTRS_CERCA_PC = 3000;
const MTRS_ENTRE_PASADAS = 3000;

const LEER_PC = (URL) => {
  return new Promise(async (resolve, reject) => {
    const OPTIONS = csv({ encoding: "utf8", separator: SEPARADOR });
    const stream = await fs.createReadStream(URL).pipe(OPTIONS);
    stream.once("data", () => {
      // console.log('INICIANDO STREAM...\n');
    });
    stream.on("data", (chunk) => LISTADO_PC.push(chunk));
    stream.on("end", () => {
      resolve();
      // console.log('FIN DEL  STREAM...\n');
    });
  });
};

const LEER_TRACKING = (URL) => {
  return new Promise(async (resolve, reject) => {
    const OPTIONS = csv({ encoding: "utf8", separator: SEPARADOR });
    const stream = await fs.createReadStream(URL).pipe(OPTIONS);
    stream.once("data", () => {
      // console.log('INICIANDO STREAM...\n');
    });
    stream.on("data", (chunk) => LISTADO_TRACKING.push(chunk));
    stream.on("end", () => {
      resolve();
    });
  });
};

//================================================================================================
const ANALIZAR_TRACKING = (LISTADO_TRACKING, LISTADO_PC) => {
  let PASADAS_PC = [];
  let MOVILES = [];
  LISTADO_TRACKING.forEach((element) => {
    const FINDED = MOVILES.find((obj) => obj === element.PPU);
    if (!FINDED) MOVILES.push(element.PPU);
  });

  let TRACK_MOVILES = {};

  MOVILES.forEach((element) => {
    const TR_FILTRADOS = LISTADO_TRACKING.filter(
      (item) => item.PPU === element
    );
    PASADAS_PC = LISTAR_PASADAS(TR_FILTRADOS, LISTADO_PC);
    TRACK_MOVILES[element] = {
      interpolaciones: FILTRAR_INTERPOLACIONES(LISTADO_PC, PASADAS_PC),
    }; //APLANANDO}
    PASADAS_PC = null;
  });

  const LISTADO_PROCESADO = PROCESAR_LISTADO_PASADAS(TRACK_MOVILES); //TODO

  // console.table(LISTADO_PROCESADO);
};

const LISTAR_PASADAS = (TR_FILTRADOS, LISTADO_PC) => {
  let LISTADO = [];

  TR_FILTRADOS.forEach((track, i) => {
    if (track.CODI_SENTI !== "-1") {
      if (TR_FILTRADOS[i + 1]) {
        const TRACK_ACT = track;
        const TRACK_POS = TR_FILTRADOS[i + 1];
        if ((TRACK_ACT.PPU, TRACK_POS.PPU)) {
          if (
            TRACK_ACT.CODI_SERVI === TRACK_POS.CODI_SERVI &&
            TRACK_ACT.CODI_SENTI === TRACK_POS.CODI_SENTI
          ) {
            let PC_SERVICIO = PC_X_SERVICIO(
              LISTADO_PC,
              TRACK_ACT.NOMB_SERVI,
              TRACK_ACT.CODI_SENTI
            );
            PC_SERVICIO.forEach((punto) => {
              let COOR_PC = {
                latitude: FORMATEAR_COODENADAS(punto.LATITUD),
                longitude: FORMATEAR_COODENADAS(punto.LONGITUD),
              };
              let COOR_ACT = {
                latitude: FORMATEAR_COODENADAS(TRACK_ACT.LATITUD),
                longitude: FORMATEAR_COODENADAS(TRACK_ACT.LONGITUD),
              };
              let COOR_POS = {
                latitude: FORMATEAR_COODENADAS(TRACK_POS.LATITUD),
                longitude: FORMATEAR_COODENADAS(TRACK_POS.LONGITUD),
              };

              let LA_PASADA = DEFINIR_PASADA(TRACK_ACT, punto);
              let DISTANCIA_1 = geolib.getDistance(COOR_PC, COOR_ACT, 1);
              let DISTANCIA_2 = geolib.getDistance(COOR_PC, COOR_POS, 1);
              // let INTERPOLARE = geolib.isPointWithinRadius(COOR_ACT, COOR_PC, 100);
              let INTERPOLARE = false;
              let CENTRO_LINEA = geolib.getCenter([COOR_ACT, COOR_POS]);
              if (DISTANCIA_1 <= DISTANCIA_2) {
                INTERPOLARE = INTERPOLAR(CENTRO_LINEA, COOR_PC, DISTANCIA_2);
              } else {
                INTERPOLARE = INTERPOLAR(CENTRO_LINEA, COOR_PC, DISTANCIA_1);
              }
              if (
                maxDistEntrePtosGPS(COOR_ACT, COOR_POS) &&
                maxTiempoEntrePtosGPS(TRACK_ACT.HORA_CHL, TRACK_POS.HORA_CHL)
              ) {
                let HORA_INTERPOLADA = GET_FECHA_INTERPOLADA(
                  TRACK_ACT,
                  TRACK_POS
                );
                LA_PASADA.PASO_HORA_CHL = HORA_INTERPOLADA.format(
                  "DD/MM/YYYY HH:mm:ss"
                );
                LA_PASADA.PASO_HORA_UTC = HORA_INTERPOLADA.add(
                  4,
                  "hours"
                ).format("DD/MM/YYYY HH:mm:ss"); //-04:00;
                let DISTACIA = parseInt(
                  geolib.getDistanceFromLine(COOR_PC, COOR_ACT, COOR_POS)
                );
                if (DISTACIA <= 130 && INTERPOLARE) {
                  LA_PASADA.V_INTERPOLADA = GET_VELOCIDAD_INTERPOLADA(
                    COOR_ACT,
                    COOR_POS,
                    TRACK_ACT.HORA_CHL,
                    TRACK_POS.HORA_CHL,
                    "kmh"
                  );
                  LISTADO.push(LA_PASADA);
                }
              }
            });
          }
        }
      }
    }
  });
  return LISTADO;
};

const FILTRAR_INTERPOLACIONES = (LISTADO_PC, PASADAS) => {
  let LISTADO_INTERPOLAR = [];
  LISTADO_PC.forEach((element) => {
    const PASADAS_CORRELATIVAS = PASADAS.filter(
      (pasada) =>
        pasada.CORRELATIVO === element.POSI_PC &&
        pasada.NOMB_SERVI === element.NOMB_SERVI
    ).sort((a, b) => a.DISTANCIA - b.DISTANCIA)[0];
    if (PASADAS_CORRELATIVAS) LISTADO_INTERPOLAR.push(PASADAS_CORRELATIVAS);
  });

  return LISTADO_INTERPOLAR;
};

const PROCESAR_LISTADO_PASADAS = (TRACK_MOVILES) => {
  // console.log(TRACK_MOVILES);

  fs.writeFileSync("ejemplo.json", JSON.stringify(TRACK_MOVILES, " ", 3));
  return TRACK_MOVILES;
  let PROCESADAS = [];
  Object.keys(TRACK_MOVILES).forEach((movil) => {
    PROCESADAS = [...PROCESADAS, ...TRACK_MOVILES[movil]];
  });
  return PROCESADAS;
};

const PC_X_SERVICIO = (LISTADO_PC, NOMB_SERVI, CODI_SENTI) => {
  let PC_SERVICIO = [];
  PC_SERVICIO = LISTADO_PC.filter(
    (item) => item.NOMB_SERVI === NOMB_SERVI && item.CODI_SENTI == CODI_SENTI
  );
  PC_SERVICIO.sort((item, i) => item.CORR_PC - i.CORR_PC);
  return PC_SERVICIO;
};

const GET_FECHA_INTERPOLADA = (TRACK_ACT, TRACK_POS) => {
  let FECHA_INTERPOLADA = null;
  let T1 = moment(TRACK_ACT.HORA_CHL, "DD/MM/YYYY HH:mm:ss");
  let T2 = moment(TRACK_POS.HORA_CHL, "DD/MM/YYYY HH:mm:ss");
  while (T1.isSameOrBefore(T2, "seconds")) {
    T1 = T1.add(1, "second");
    T2 = T2.subtract(1, "second");
  }
  FECHA_INTERPOLADA = moment(T1, "DD/MM/YYYY HH:mm:ss");
  //   .format(
  //     "DD/MM/YYYY HH:mm:ss"
  //   );

  return FECHA_INTERPOLADA;
};

const GET_VELOCIDAD_INTERPOLADA = (P1, P2, T1, T2, UNIDAD) => {
  let VELOCIDAD = 0;
  P1.time = moment(T1, "DD/MM/YYYY HH:mm:ss").toDate().getTime();
  P2.time = moment(T2, "DD/MM/YYYY HH:mm:ss").toDate().getTime();
  VELOCIDAD = geolib.convertSpeed(geolib.getSpeed(P1, P2), UNIDAD);
  return parseInt(VELOCIDAD);
};

const DEFINIR_PASADA = (track, punto) => {
  let PASADA = {
    IDDE: null,
    DNI1: track.DNI1,
    DNI2: track.DNI2,
    PERIODO: track.PERI_MES,
    ID_SERVI: track.CODI_SERVI,
    NOMB_SERVI: track.NOMB_SERVI,
    CODI_SENTI: track.CODI_SENTI,

    PPU: track.PPU,
    CODI_EXPE: null,
    FECH_CHI: null,
    FECH_UTC: null,
    CORRELATIVO: punto.POSI_PC,
    LATITUD: punto.LATITUD,
    LONGITUD: punto.LONGITUD,
    V_INTERPOLADA: null,
    PASO_HORA_CHL: null,
    PASO_HORA_UTC: null,
    PERIODO_HORA: null,
    INVALIDA: 1,
    EXTRA: 0,
  };

  return PASADA;
};

const INTERPOLAR = (COOR, COOR_PC, RADIO) => {
  let INTERPOLAR = geolib.isPointWithinRadius(COOR, COOR_PC, RADIO * 0.5145);
  return INTERPOLAR;
};

//INICIO: CONDICIONES PARA PROCEDER A INTERPOLAR HORAS DE PUNTOS GPS
// const MaxVelLineaPtosGpsUrbano = (TRACK_ANT, TRACK_POS, TIPO_PUNTO) => {
//     const VELO_MAX = 72;
//     let VALIDO = false;
//     //SI VELOCIDAD_LINEAL <= VELO_MAX = TRUE
//     return VALIDO;
// }

// const MaxVelLineaPtosGpsRural = (COOR_1, COOR_2, FECHA_1, FECHA_2, TIPO_PUNTO) => {
//     const VELO_MAX = 100;
//     let VALIDO = false;
//     //SI VELOCIDAD_LINEAL <= VELO_MAX = TRUE
//     return VALIDO;
// }

const maxTiempoEntrePtosGPS = (FECHA_1, FECHA_2) => {
  let VALIDO = false;
  const start = moment(FECHA_1, "YYYY-mm-dd HH:mm:ss");
  const end = moment(FECHA_2, "YYYY-mm-dd HH:mm:ss");
  const DIFERENCIA = moment.duration(end.diff(start)).asMinutes();
  if (DIFERENCIA <= 5) {
    VALIDO = true;
  }
  return VALIDO;
};

const maxDistEntrePtosGPS = (COOR_ACT, COOR_POS) => {
  let VALIDO = false;
  let DISTANCIA = geolib.getDistance(COOR_ACT, COOR_POS);
  if (DISTANCIA < MTRS_ENTRE_PASADAS) {
    VALIDO = true;
  }
  return VALIDO;
};
//FIN: CONDICIONES PARA PROCEDER A INTERPOLAR HORAS DE PUNTOS GPS

const FORMATEAR_COODENADAS = (LatLng) => {
  //DA FORMATO A LAS CORDENADAS GPS Ej: -23,123456 to -23.123456
  return LatLng.replace(/,/g, ".");
};

const main = async () => {
  console.time("FIN");
  await LEER_PC(URL_PC);
  await LEER_TRACKING(URL_TRACKING);

  if (LISTADO_TRACKING.length > 0) {
    let SERVICIOS = [];
    const TR_FILTRADOS = LISTADO_TRACKING.filter(
      (item) => item.PPU === "ZP9889"
    );
    // const TR_FILTRADOS = LISTADO_TRACKING.filter((item) => item.PPU === "JXWH40");
    const PC_FILTRADOS = LISTADO_PC.filter(
      (item) => item.NOMB_SERVI === "104N" && item.CODI_SENTI === "1"
    );
    ANALIZAR_TRACKING(LISTADO_TRACKING, LISTADO_PC);
    // ANALIZAR_TRACKING(TR_FILTRADOS, LISTADO_PC);
    console.timeEnd("FIN");
  }
};

main();
