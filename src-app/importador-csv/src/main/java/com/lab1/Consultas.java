package com.lab1;

import java.sql.*;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/**
 * Clase Consultas.
 *
 * Proporciona métodos para realizar diversas consultas a la base de datos de estaciones de servicio.
 */
@Slf4j
public class Consultas {

    private final Connection conn;

    // Constantes para valores repetidos
    private static final String DATABASE = "estaciones_servicio";
    private static final String TIPO_ESTACION_TERRESTRE = "Terrestre";
    private static final String TIPO_ESTACION_MARITIMA = "Puerto";
    private static final String COMBUSTIBLE_GASOLINA_95 = "gasolina 95 E5";
    private static final String COMBUSTIBLE_GASOLEO_A = "gasóleo A";
    private static final String PROVINCIA_MADRID = "MADRID";
    private static final String MUNICIPIO_ALBACETE = "ALBACETE";

    // Consultas SQL actualizadas según las modificaciones de las tablas
    private static final String SQL_EMPRESA_MAS_ESTACIONES =
            "SELECT m.nombre_marca, COUNT(es.estacion_id) AS num_estaciones " +
                    "FROM estacion_servicio es " +
                    "JOIN marca m ON es.marca_id = m.marca_id " +
                    "JOIN tipo_estacion te ON es.tipo_estacion_id = te.tipo_estacion_id " +
                    "WHERE te.tipo_estacion = ? " +
                    "GROUP BY m.nombre_marca " +
                    "ORDER BY num_estaciones DESC " +
                    "LIMIT 1";

    private static final String SQL_ESTACION_MAS_BARATA_PROVINCIA_COMBUSTIBLE =
            "SELECT es.direccion, m.nombre_marca, mg.nombre_margen, MIN(pc.precio) AS precio_minimo " +
                    "FROM precio_combustible pc " +
                    "JOIN estacion_servicio es ON pc.estacion_id = es.estacion_id " +
                    "JOIN marca m ON es.marca_id = m.marca_id " +
                    "JOIN margen mg ON es.margen_id = mg.margen_id " +
                    "JOIN codigo_postal cp ON es.codigo_postal_id = cp.codigo_postal_id " +
                    "JOIN localidad l ON cp.localidad_id = l.localidad_id " +
                    "JOIN municipio mu ON l.municipio_id = mu.municipio_id " +
                    "JOIN provincia p ON mu.provincia_id = p.provincia_id " +
                    "JOIN tipo_combustible tc ON pc.combustible_id = tc.combustible_id " +
                    "WHERE p.nombre_provincia = ? AND tc.nombre_combustible = ? " +
                    "GROUP BY es.estacion_id " +
                    "ORDER BY precio_minimo ASC " +
                    "LIMIT 1";

    private static final String SQL_ESTACION_MAS_BARATA_RADIO =
            "SELECT es.direccion, m.nombre_marca, mg.nombre_margen, pc.precio, " +
                    "ST_Distance_Sphere(es.ubicacion, ST_GeomFromText(CONCAT('POINT(', ?, ' ', ?, ')'), 4326)) / 1000 AS distancia_km " +
                    "FROM precio_combustible pc " +
                    "JOIN estacion_servicio es ON pc.estacion_id = es.estacion_id " +
                    "JOIN marca m ON es.marca_id = m.marca_id " +
                    "JOIN margen mg ON es.margen_id = mg.margen_id " +
                    "JOIN codigo_postal cp ON es.codigo_postal_id = cp.codigo_postal_id " +
                    "JOIN localidad l ON cp.localidad_id = l.localidad_id " +
                    "JOIN municipio mu ON l.municipio_id = mu.municipio_id " +
                    "JOIN tipo_combustible tc ON pc.combustible_id = tc.combustible_id " +
                    "WHERE tc.nombre_combustible = ? AND mu.nombre_municipio = ? " +
                    "HAVING distancia_km <= ? " +
                    "ORDER BY pc.precio ASC " +
                    "LIMIT 1";

    private static final String SQL_PROVINCIA_GASOLINA_MAS_CARA_MARITIMA =
            "SELECT p.nombre_provincia, pc.precio " +
                    "FROM precio_combustible pc " +
                    "JOIN estacion_servicio es ON pc.estacion_id = es.estacion_id " +
                    "JOIN tipo_estacion te ON es.tipo_estacion_id = te.tipo_estacion_id " +
                    "JOIN codigo_postal cp ON es.codigo_postal_id = cp.codigo_postal_id " +
                    "JOIN localidad l ON cp.localidad_id = l.localidad_id " +
                    "JOIN municipio mu ON l.municipio_id = mu.municipio_id " +
                    "JOIN provincia p ON mu.provincia_id = p.provincia_id " +
                    "JOIN tipo_combustible tc ON pc.combustible_id = tc.combustible_id " +
                    "WHERE te.tipo_estacion = ? AND tc.nombre_combustible = ? " +
                    "ORDER BY pc.precio DESC " +
                    "LIMIT 1";

    /**
     * Constructor de la clase Consultas.
     *
     * @param conn Conexión a la base de datos.
     */
    public Consultas(final Connection conn) {
        this.conn = conn;
    }

    /**
     * Método auxiliar para ejecutar consultas SQL con parámetros y procesar el ResultSet.
     *
     * @param sql               Consulta SQL a ejecutar.
     * @param resultadoProcessor Función que procesa el ResultSet obtenido.
     * @param parametros        Parámetros para el PreparedStatement.
     * @throws ConsultasException Si ocurre un error al ejecutar la consulta.
     */
    private void ejecutarConsulta(String sql, Consumer<ResultSet> resultadoProcessor, Object... parametros) throws ConsultasException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < parametros.length; i++) {
                pstmt.setObject(i + 1, parametros[i]);
            }
            try (ResultSet rs = pstmt.executeQuery()) {
                resultadoProcessor.accept(rs);
            }
        } catch (SQLException e) {
            log.error("Error al ejecutar la consulta: {}", sql, e);
            throw new ConsultasException("No se pudo ejecutar la consulta", e);
        }
    }

    /**
     * Obtiene la empresa con más estaciones de un tipo específico.
     *
     * @param tipoEstacion Tipo de estación (por ejemplo, "Terrestre" o "Marítima").
     * @throws ConsultasException Si ocurre un error al ejecutar la consulta.
     */
    private void obtenerEmpresaConMasEstaciones(final String tipoEstacion) throws ConsultasException {
        ejecutarConsulta(SQL_EMPRESA_MAS_ESTACIONES, rs -> {
            try {
                if (rs.next()) {
                    final String nombreMarca = rs.getString("nombre_marca");
                    final int numEstaciones = rs.getInt("num_estaciones");
                    log.info("La empresa con más estaciones {} es {} con {} estaciones.", tipoEstacion, nombreMarca, numEstaciones);
                } else {
                    log.info("No se encontraron resultados para estaciones {}.", tipoEstacion);
                }
            } catch (SQLException e) {
                log.error("Error al procesar los resultados para obtenerEmpresaConMasEstaciones", e);
            }
        }, tipoEstacion);
    }

    /**
     * Obtiene la estación más barata para un combustible específico en una provincia.
     *
     * @param provincia   Nombre de la provincia.
     * @param combustible Nombre del combustible.
     * @throws ConsultasException Si ocurre un error al ejecutar la consulta.
     */
    private void obtenerEstacionMasBarataEnProvincia(final String provincia, final String combustible) throws ConsultasException {
        ejecutarConsulta(SQL_ESTACION_MAS_BARATA_PROVINCIA_COMBUSTIBLE, rs -> {
            try {
                if (rs.next()) {
                    final String direccion = rs.getString("direccion");
                    final String nombreMarca = rs.getString("nombre_marca");
                    final String margen = rs.getString("nombre_margen");
                    final double precio = rs.getDouble("precio_minimo");
                    log.info("Estación más barata en {} para {}:", provincia, combustible);
                    log.info("Dirección: {}", direccion);
                    log.info("Empresa: {}", nombreMarca);
                    log.info("Margen: {}", margen);
                    log.info("Precio: {}", precio);
                } else {
                    log.info("No se encontraron resultados en {}.", provincia);
                }
            } catch (SQLException e) {
                log.error("Error al procesar los resultados para obtenerEstacionMasBarataEnProvincia", e);
            }
        }, provincia, combustible);
    }

    /**
     * Obtiene la estación más barata para un combustible específico dentro de un radio desde un punto.
     *
     * @param municipio    Nombre del municipio.
     * @param combustible  Nombre del combustible.
     * @param centroLat    Latitud del centro.
     * @param centroLon    Longitud del centro.
     * @param radioKm      Radio en kilómetros.
     * @throws ConsultasException Si ocurre un error al ejecutar la consulta.
     */
    private void obtenerEstacionMasBarataEnRadio(final String municipio, final String combustible, final double centroLat, final double centroLon, final double radioKm) throws ConsultasException {
        ejecutarConsulta(SQL_ESTACION_MAS_BARATA_RADIO, rs -> {
            try {
                if (rs.next()) {
                    final String direccion = rs.getString("direccion");
                    final String nombreMarca = rs.getString("nombre_marca");
                    final String margen = rs.getString("nombre_margen");
                    final double precio = rs.getDouble("precio");
                    final double distanciaKm = rs.getDouble("distancia_km");
                    String distanciaFormateada = String.format("%.3f", distanciaKm);
                    log.info("Estación más barata en un radio de {} km en {} para {}:", radioKm, municipio, combustible);
                    log.info("Dirección: {}", direccion);
                    log.info("Empresa: {}", nombreMarca);
                    log.info("Margen: {}", margen);
                    log.info("Precio: {}", precio);
                    log.info("Distancia: {} km", distanciaFormateada);
                } else {
                    log.info("No se encontraron estaciones en un radio de {} km en {}.", radioKm, municipio);
                }
            } catch (SQLException e) {
                log.error("Error al procesar los resultados para obtenerEstacionMasBarataEnRadio", e);
            }
        }, centroLon, centroLat, combustible, municipio, radioKm);
    }

    /**
     * Obtiene la provincia con la estación marítima que tiene la Gasolina 95 E5 más cara.
     *
     * @throws ConsultasException Si ocurre un error al ejecutar la consulta.
     */
    private void obtenerProvinciaConGasolina95MasCaraEnMaritima() throws ConsultasException {
        ejecutarConsulta(SQL_PROVINCIA_GASOLINA_MAS_CARA_MARITIMA, rs -> {
            try {
                if (rs.next()) {
                    final String nombreProvincia = rs.getString("nombre_provincia");
                    final double precio = rs.getDouble("precio");
                    log.info("La provincia con la estación marítima con la Gasolina 95 E5 más cara es {} con un precio de {}", nombreProvincia, precio);
                } else {
                    log.info("No se encontraron resultados para Gasolina 95 en estaciones marítimas.");
                }
            } catch (SQLException e) {
                log.error("Error al procesar los resultados para obtenerProvinciaConGasolina95MasCaraEnMaritima", e);
            }
        }, TIPO_ESTACION_MARITIMA, COMBUSTIBLE_GASOLINA_95);
    }

    /**
     * Ejecuta todas las consultas definidas en la clase.
     *
     * @throws ConsultasException Si ocurre un error al ejecutar alguna consulta.
     */
    public void ejecutarConsultas() throws ConsultasException {
        obtenerEmpresaConMasEstaciones(TIPO_ESTACION_TERRESTRE);
        obtenerEmpresaConMasEstaciones(TIPO_ESTACION_MARITIMA);
        obtenerEstacionMasBarataEnProvincia(PROVINCIA_MADRID, COMBUSTIBLE_GASOLINA_95);
        obtenerEstacionMasBarataEnRadio(MUNICIPIO_ALBACETE, COMBUSTIBLE_GASOLEO_A, 38.994349, -1.85643, 10);
        obtenerProvinciaConGasolina95MasCaraEnMaritima();
    }

    /**
     * Método principal que inicia la aplicación.
     *
     * @param args Argumentos de línea de comandos (host y nombre de la base de datos).
     */
    public static void main(final String[] args) {
        // Obtener el host y la base de datos de los argumentos o usar valores por defecto
        final String host = args.length > 0 ? args[0] : "localhost";
        final String database = args.length > 1 ? args[1] : DATABASE;

        // Crear una instancia de MySqlConnector y obtener la conexión
        try (Connection conn = new MySqlConnector(host, database).getConnection()) {
            // Instanciar la clase Consultas y ejecutar las consultas
            final Consultas consultas = new Consultas(conn);
            consultas.ejecutarConsultas();
        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }
}
