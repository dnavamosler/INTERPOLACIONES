const geolib = require("geolib");
// const {createObjectCsvWriter} = require("csv-writer");
const { v4: uuid } = require("uuid");
const csv = require("csv-parser");
const fs = require("fs");

const moment = require("moment");
const crearArchivos = require("./AUXILIARES/crear-archivo");
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
  LISTADO_TRACKING.forEach(({ PPU, HORA_CHL, NOMB_SERVI }) => {
    const FINDED = MOVILES.find(
      (obj) =>
        obj.patente === PPU &&
        moment(HORA_CHL, "DD/MM/YYYY HH:mm:ss").isSame(obj.dia, "days") &&
        NOMB_SERVI == obj.NOMB_SERVI
    );

    if (!FINDED)
      MOVILES.push({
        patente: PPU,
        dia: moment(HORA_CHL, "DD/MM/YYYY HH:mm:ss").format("YYYY-MM-DD"),
        NOMB_SERVI,

        UUID: `${PPU}|${moment(HORA_CHL, "DD/MM/YYYY HH:mm:ss").format(
          "YYYY-MM-DD"
        )}|${NOMB_SERVI}`,
      });
  });

  // RETIRAR SERVICIOS NO COMERCIALES
  MOVILES = MOVILES.filter((item) => item.CODI_SENTI !== "-1");
  MOVILES = CONSEGUIR_REGISTROS_UNICOS(MOVILES);
  let TRACK_MOVILES = {};

  MOVILES.forEach((element) => {
    // const TR_FILTRADOS = LISTADO_TRACKING.filter(
    //   (item) =>
    //     `${item.PPU}|${moment(item.HORA_CHL, "DD/MM/YYYY HH:mm:ss").format(
    //       "YYYY-MM-DD"
    //     )}|${item.NOMB_SERVI}` === element.UUID
    // ); //filtrar tracking de movil en un dia determinado y un servicio determinado

    let GRUPOS = {};

    Object.keys(element.grupos).forEach((grupoID) => {
      const grupo = element.grupos[grupoID];
      PASADAS_PC = LISTAR_PASADAS(grupo.registros, LISTADO_PC);

      GRUPOS[grupoID] = {
        ...grupo,
        interpolaciones: FILTRAR_INTERPOLACIONES(LISTADO_PC, PASADAS_PC),
      };
    });

    TRACK_MOVILES[element.UUID] = {
      ...element,
      grupos: GRUPOS,
    }; //APLANANDO}
  });

  const DATA_FINAL = PROCESAR_LISTADO_PASADAS(TRACK_MOVILES); //TODO

  crearArchivos({
    headers: [
      { id: "IDDE", title: "IDDE" },
      { id: "DNI1", title: "DNI1" },
      { id: "DNI2", title: "DNI2" },
      { id: "PERI_MES", title: "PERI_MES" },
      { id: "ID_SERVI", title: "ID_SERVI" },
      { id: "NOMB_SERVI", title: "NOMB_SERVI" },
      { id: "CODI_SENTI", title: "CODI_SENTI" },
      { id: "PPU", title: "PPU" },
      { id: "CODI_EXPE", title: "CODI_EXPE" },
      { id: "HORA_CHL", title: "HORA_CHL" },
      { id: "HORA_UTC", title: "HORA_UTC" },
      { id: "CORRELATIVO", title: "CORRELATIVO" },
      { id: "LATITUD", title: "LATITUD" },
      { id: "LONGITUD", title: "LONGITUD" },
      { id: "V_INTERPOLADA", title: "V_INTERPOLADA" },
      { id: "PASO_HORA_CHL", title: "PASO_HORA_CHL" },
      { id: "PASO_HORA_UTC", title: "PASO_HORA_UTC" },
      { id: "PERIODO_HORA", title: "PERIODO_HORA" },
      { id: "INVALIDA", title: "INVALIDA" },
      { id: "EXTRA", title: "EXTRA" },
    ],
    records: DATA_FINAL,
    pathResult: `SALIDAS/TEST-1.csv`,
  });
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

  return LISTADO_INTERPOLAR.filter(
    (v, i, a) => a.findIndex((t) => t.CORRELATIVO === v.CORRELATIVO) === i
  );
};

const PROCESAR_LISTADO_PASADAS = (TRACK_MOVILES) => {
  // console.log(TRACK_MOVILES);

  fs.writeFileSync("GRUPOS.json", JSON.stringify(TRACK_MOVILES, " ", 3));

  let PROCESADAS = [];
  Object.keys(TRACK_MOVILES).forEach((movil) => {
    //ACCESO A MOVIL

    Object.keys(TRACK_MOVILES[movil].grupos).forEach((grupo) => {
      //ACCESO A GRUPO
      const MAIN_SERVICE =
        TRACK_MOVILES[movil].grupos[grupo].SERVICIO_PRINCIPAL;
      const INTERPOLACIONES =
        TRACK_MOVILES[movil].grupos[grupo].interpolaciones;
      PROCESADAS = [
        ...PROCESADAS,
        ...INTERPOLACIONES.map((int) => {
          return {
            ...MAIN_SERVICE,
            CORRELATIVO: int.CORRELATIVO,
            LATITUD: int.LATITUD,
            LONGITUD: int.LONGITUD,
            PASO_HORA_CHL: int.PASO_HORA_CHL,
            PASO_HORA_UTC: int.PASO_HORA_UTC,
            INVALIDA: VALIDAR_INTERPOLACION(MAIN_SERVICE, INTERPOLACIONES), //revisar
            ID_SERVI: MAIN_SERVICE.CODI_SERVI,
            IDDE: `${MAIN_SERVICE.IDDE}${int.CORRELATIVO}`,
            V_INTERPOLADA: int.V_INTERPOLADA,
            EXTRA: 0,
          };
        }),
      ];
    });
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

const VALIDAR_INTERPOLACION = (SERVICIO, INTERPOLACIONES) => {
  const puntos = LISTADO_PC.filter(
    (item) =>
      item.NOMB_SERVI == SERVICIO.NOMB_SERVI &&
      item.CODI_SENTI == SERVICIO.CODI_SENTI
  );

  const puntoInicial = puntos[0].POSI_PC;
  const puntoFinal = puntos[puntos.length - 1].POSI_PC;
  const RESTANTE = puntos.length - 2;
  const INTERPOLACIONES_FILTRADAS = INTERPOLACIONES.filter(
    (item) => item.CORRELATIVO != puntoInicial && item.CORRELATIVO != puntoFinal
  );

  const PRIMERA_VALIDACION =
    INTERPOLACIONES.some((item) => item.CORRELATIVO == puntoInicial) &&
    INTERPOLACIONES.some((item) => item.CORRELATIVO == puntoFinal)
      ? true
      : false;

  // RESTANTE ---> 100%
  // INTERPOLACIONES_FILTRADAS ---> x
  const porcentajeCumplimiento =
    (INTERPOLACIONES_FILTRADAS.length * 100) / RESTANTE;
  const SEGUNDA_VALIDACION = porcentajeCumplimiento >= 80;

  return PRIMERA_VALIDACION && SEGUNDA_VALIDACION ? "0" : "1";
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

const CONSEGUIR_REGISTROS_UNICOS = (MOVILES) => {
  return MOVILES.map((movil) => {
    const TR_FILTRADOS = LISTADO_TRACKING.filter(
      (item) => item.CODI_SENTI != "-1"
    ).filter(
      (item) =>
        `${item.PPU}|${moment(item.HORA_CHL, "DD/MM/YYYY HH:mm:ss").format(
          "YYYY-MM-DD"
        )}|${item.NOMB_SERVI}` === movil.UUID
    ); //filtrar tracking de movil en un dia determinado y un servicio determinado

    let grupoServicios = {};
    // SEPARACION EN SENTIDO IDA-VUELTA
    let currentSentido = null;
    let registroActual = null;
    TR_FILTRADOS.forEach((item, i) => {
      const SENTIDO_ACTUAL = item.CODI_SENTI;
      const HORA = item.HORA_CHL;
      const SERVICIO_PRINCIPAL = {
        DNI1: item.DNI1,
        DNI2: item.DNI2,
        PERI_MES: item.PERI_MES,
        CODI_SERVI: item.CODI_SERVI,
        NOMB_SERVI: item.NOMB_SERVI,
        CODI_SENTI: item.CODI_SENTI,
        PPU: item.PPU,
        HORA_CHL: item.HORA_CHL,
        HORA_UTC: item.HORA_UTC,
        CODI_EXPE: (
          new Date(
            moment(item.HORA_CHL, "DD/MM/YYYY HH:mm:ss").format(
              "MM/DD/YYYY HH:mm:ss"
            )
          ).getTime() / 1000
        )
          .toString()
          .slice(2, 10),
        IDDE: `${item.PPU}-${item.HORA_CHL}-`,
        PERIODO_HORA: moment(item.HORA_CHL, "DD/MM/YYYY HH:mm:ss").format("HH"),
      };
      if (i == 0) {
        const id = uuid();
        registroActual = id;
        currentSentido = SENTIDO_ACTUAL;
        //   CREACION DE PRIMER REGISTRO
        grupoServicios[id] = {
          inicio: HORA,
          SERVICIO_PRINCIPAL,
          sentido: currentSentido,
          registros: [item],
        };
      } else {
        // anexarRegistro si el sentido es igual
        if (currentSentido == SENTIDO_ACTUAL) {
          grupoServicios[registroActual] = {
            ...grupoServicios[registroActual],
            registros: [...grupoServicios[registroActual].registros, item],
          };
        } else {
          // crear nuevo registro
          const id = uuid();
          registroActual = id;
          currentSentido = SENTIDO_ACTUAL;

          //   CREACION DE PRIMER REGISTRO
          grupoServicios[id] = {
            inicio: HORA,
            SERVICIO_PRINCIPAL,
            sentido: currentSentido,
            registros: [item],
          };
        }
      }
    });

    return { ...movil, grupos: grupoServicios };
  });
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
