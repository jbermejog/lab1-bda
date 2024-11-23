package com.lab1;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Locale;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;

/**
 * Clase ImportadorCSV.
 *
 * <p>
 * Esta clase se encarga de importar datos desde archivos CSV ubicados en un directorio específico
 * a una base de datos MySQL. Implementa varias optimizaciones para mejorar el rendimiento, como
 * la reutilización de PreparedStatements, implementación de batch inserts y cacheo de IDs ya existentes.
 * </p>
 */
@Slf4j
public class ImportadorCSV {

    /**
     * Directorio donde se encuentran los archivos CSV a procesar.
     */
    private static final String CSV_DIRECTORY = "ficheroscsv";

    /**
     * Nombre de la base de datos a utilizar.
     */
    private static final String DATABASE = "estaciones_servicio";

    /**
     * Conexión a la base de datos.
     */
    private static Connection conn;

    // Declaración de PreparedStatements reutilizables

    private static PreparedStatement selectProvinciaStmt;
    private static PreparedStatement insertProvinciaStmt;
    private static PreparedStatement selectMunicipioStmt;
    private static PreparedStatement insertMunicipioStmt;
    private static PreparedStatement selectLocalidadStmt;
    private static PreparedStatement insertLocalidadStmt;
    private static PreparedStatement selectCodigoPostalStmt;
    private static PreparedStatement insertCodigoPostalStmt;
    private static PreparedStatement selectMargenStmt;
    private static PreparedStatement insertMargenStmt;
    private static PreparedStatement selectHorarioStmt;
    private static PreparedStatement insertHorarioStmt;
    private static PreparedStatement selectMarcaStmt;
    private static PreparedStatement insertMarcaStmt;
    private static PreparedStatement selectTipoEstacionStmt;
    private static PreparedStatement insertTipoEstacionStmt;
    private static PreparedStatement selectEstacionServicioStmt;
    private static PreparedStatement insertEstacionServicioStmt;
    private static PreparedStatement selectCombustibleStmt;
    private static PreparedStatement insertCombustibleStmt;
    private static PreparedStatement selectPrecioCombustibleStmt;
    private static PreparedStatement insertPrecioCombustibleStmt;

    // Mapas para cachear IDs y evitar consultas redundantes

    private static Map<String, Integer> provinciaCache = new HashMap<>();
    private static Map<String, Integer> municipioCache = new HashMap<>();
    private static Map<String, Integer> localidadCache = new HashMap<>();
    private static Map<String, Integer> codigoPostalCache = new HashMap<>();
    private static Map<String, Integer> margenCache = new HashMap<>();
    private static Map<String, Integer> horarioCache = new HashMap<>();
    private static Map<String, Integer> marcaCache = new HashMap<>();
    private static Map<String, Integer> tipoEstacionCache = new HashMap<>();
    private static Map<String, Integer> combustibleCache = new HashMap<>();
    private static Map<String, Integer> estacionServicioCache = new HashMap<>();

    /**
     * Método principal que inicia la importación de los archivos CSV a la base de datos.
     *
     * @param args Argumentos de línea de comandos: host y nombre de la base de datos (opcional).
     */
    public static void main(String[] args) {
        try {
            // Obtener el host y la base de datos de los argumentos o usar valores por defecto
            String host = args.length > 0 ? args[0] : "localhost";
            String database = args.length > 1 ? args[1] : DATABASE;

            // Crear una instancia de MySqlConnector facilitado por el profesor
            MySqlConnector mySqlConnector = new MySqlConnector(host, database);
            conn = mySqlConnector.getConnection();

            // Preparar los PreparedStatements
            prepareStatements();

            // Desactivar auto-commit para controlar manualmente las transacciones
            conn.setAutoCommit(false);

            // Obtener la lista de archivos CSV en el directorio especificado
            File folder = new File(CSV_DIRECTORY);
            File[] listOfFiles = folder.listFiles((dir, name) -> name.endsWith(".csv"));

            if (listOfFiles != null) {
                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        log.info("Procesando archivo CSV: {}", file.getName());
                        processCSVFile(file);
                    }
                }
                // Confirmar transacción después de procesar todos los archivos
                conn.commit();
                conn.setAutoCommit(true);
            } else {
                log.warn("No se encontraron archivos CSV en el directorio {}", CSV_DIRECTORY);
            }

            // Cerrar los PreparedStatements
            closeStatements();

            // Cerrar la conexión a la base de datos
            conn.close();
        } catch (Exception e) {
            log.error("Error al importar archivos CSV", e);
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.rollback();
                    log.info("Transacción revertida debido a un error.");
                }
            } catch (SQLException rollbackEx) {
                log.error("Error al hacer rollback de la transacción", rollbackEx);
            }
        }
    }

    /**
     * Prepara los PreparedStatements reutilizables para las operaciones de base de datos.
     *
     * @throws SQLException Si ocurre un error al preparar los statements.
     */
    private static void prepareStatements() throws SQLException {
        // Preparación de los PreparedStatements reutilizables

        // Statements para 'provincia'
        selectProvinciaStmt = conn.prepareStatement("SELECT provincia_id FROM provincia WHERE nombre_provincia = ?");
        insertProvinciaStmt = conn.prepareStatement(
                "INSERT INTO provincia (nombre_provincia) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'municipio'
        selectMunicipioStmt = conn.prepareStatement(
                "SELECT municipio_id FROM municipio WHERE nombre_municipio = ? AND provincia_id = ?");
        insertMunicipioStmt = conn.prepareStatement(
                "INSERT INTO municipio (nombre_municipio, provincia_id) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'localidad'
        selectLocalidadStmt = conn.prepareStatement(
                "SELECT localidad_id FROM localidad WHERE nombre_localidad = ? AND municipio_id = ?");
        insertLocalidadStmt = conn.prepareStatement(
                "INSERT INTO localidad (nombre_localidad, municipio_id) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'codigo_postal'
        selectCodigoPostalStmt = conn.prepareStatement(
                "SELECT codigo_postal_id FROM codigo_postal WHERE codigo_postal = ? AND localidad_id = ?");
        insertCodigoPostalStmt = conn.prepareStatement(
                "INSERT INTO codigo_postal (codigo_postal, localidad_id) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'margen'
        selectMargenStmt = conn.prepareStatement("SELECT margen_id FROM margen WHERE nombre_margen = ?");
        insertMargenStmt = conn.prepareStatement(
                "INSERT INTO margen (nombre_margen) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'horario'
        selectHorarioStmt = conn.prepareStatement("SELECT horario_id FROM horario WHERE descripcion_horario = ?");
        insertHorarioStmt = conn.prepareStatement(
                "INSERT INTO horario (descripcion_horario) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'marca'
        selectMarcaStmt = conn.prepareStatement("SELECT marca_id FROM marca WHERE nombre_marca = ?");
        insertMarcaStmt = conn.prepareStatement(
                "INSERT INTO marca (nombre_marca) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'tipo_estacion'
        selectTipoEstacionStmt = conn.prepareStatement("SELECT tipo_estacion_id FROM tipo_estacion WHERE tipo_estacion = ?");
        insertTipoEstacionStmt = conn.prepareStatement(
                "INSERT INTO tipo_estacion (tipo_estacion) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'estacion_servicio'

        // Validación de existencia de estación con ubicación espacial
        selectEstacionServicioStmt = conn.prepareStatement(
                "SELECT estacion_id FROM estacion_servicio WHERE ST_Equals(ubicacion, ST_PointFromText(?, 4326))"
        );

        // Inserción de una estación con campo 'ubicacion' espacial
        insertEstacionServicioStmt = conn.prepareStatement(
                "INSERT INTO estacion_servicio (direccion, margen_id, ubicacion, codigo_postal_id, marca_id, horario_id, tipo_estacion_id) " +
                        "VALUES (?, ?, ST_PointFromText(?, 4326), ?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS
        );

        // Statements para 'tipo_combustible'
        selectCombustibleStmt = conn.prepareStatement("SELECT combustible_id FROM tipo_combustible WHERE nombre_combustible = ?");
        insertCombustibleStmt = conn.prepareStatement(
                "INSERT INTO tipo_combustible (nombre_combustible) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        // Statements para 'precio_combustible'
        selectPrecioCombustibleStmt = conn.prepareStatement(
                "SELECT precio_id FROM precio_combustible WHERE estacion_id = ? AND combustible_id = ? AND fecha_hora = ?");
        insertPrecioCombustibleStmt = conn.prepareStatement(
                "INSERT INTO precio_combustible (estacion_id, combustible_id, precio, fecha_hora) VALUES (?, ?, ?, ?)");
    }

    /**
     * Cierra los PreparedStatements y libera los recursos asociados.
     *
     * @throws SQLException Si ocurre un error al cerrar los statements.
     */
    private static void closeStatements() throws SQLException {
        // Cierre de los PreparedStatements
        selectProvinciaStmt.close();
        insertProvinciaStmt.close();
        selectMunicipioStmt.close();
        insertMunicipioStmt.close();
        selectLocalidadStmt.close();
        insertLocalidadStmt.close();
        selectCodigoPostalStmt.close();
        insertCodigoPostalStmt.close();
        selectMargenStmt.close();
        insertMargenStmt.close();
        selectHorarioStmt.close();
        insertHorarioStmt.close();
        selectMarcaStmt.close();
        insertMarcaStmt.close();
        selectTipoEstacionStmt.close();
        insertTipoEstacionStmt.close();
        selectEstacionServicioStmt.close();
        insertEstacionServicioStmt.close();
        selectCombustibleStmt.close();
        insertCombustibleStmt.close();
        selectPrecioCombustibleStmt.close();
        insertPrecioCombustibleStmt.close();
    }

    /**
     * Procesa un archivo CSV, leyendo sus registros y agregándolos a la base de datos.
     *
     * @param file El archivo CSV a procesar.
     */
    private static void processCSVFile(File file) {
        try (CSVReader csvReader = new CSVReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String[] headers = csvReader.readNext();

            if (headers == null) {
                log.warn("El archivo CSV {} está vacío.", file.getName());
                return;
            }

            // Crear mapa de índices de columnas para acceder a los datos por nombre
            Map<String, Integer> columnIndices = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                columnIndices.put(headers[i].trim(), i);
            }

            List<String[]> batchData = new ArrayList<>();
            int batchSize = 5000;
            int count = 0;

            String[] data;
            while ((data = csvReader.readNext()) != null) {
                batchData.add(data);
                count++;

                if (count % batchSize == 0) {
                    processBatch(batchData, headers, columnIndices);
                    batchData.clear();
                }
            }
            if (!batchData.isEmpty()) {
                processBatch(batchData, headers, columnIndices);
            }
        } catch (IOException | CsvValidationException e) {
            log.error("Error al procesar el archivo CSV: {}", file.getName(), e);
        }
    }

    /**
     * Procesa un lote de registros, insertándolos o actualizándolos en la base de datos.
     *
     * @param batchData     Lista de registros a procesar.
     * @param headers       Encabezados del CSV.
     * @param columnIndices Mapa de índices de columnas.
     */
    private static void processBatch(List<String[]> batchData, String[] headers, Map<String, Integer> columnIndices) {
        try {
            for (String[] data : batchData) {
                processRecord(data, headers, columnIndices);
            }

            // Ejecutar batch inserts después de procesar el batch
            insertPrecioCombustibleStmt.executeBatch();
            insertPrecioCombustibleStmt.clearBatch();

            log.info("Procesados {} registros", batchData.size());
        } catch (SQLException e) {
            log.error("Error al procesar el batch de registros", e);
            try {
                conn.rollback();
                log.info("Transacción revertida debido a un error en el batch.");
            } catch (SQLException rollbackEx) {
                log.error("Error al hacer rollback de la transacción", rollbackEx);
            }
        }
    }

    /**
     * Procesa un registro individual, realizando las inserciones o actualizaciones necesarias.
     *
     * @param data          Datos del registro.
     * @param headers       Encabezados del CSV.
     * @param columnIndices Mapa de índices de columnas.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static void processRecord(String[] data, String[] headers, Map<String, Integer> columnIndices) throws SQLException {
        // Extraer datos de la línea CSV usando el mapa de índices
        String provincia = data[columnIndices.get("Provincia")].trim();
        String municipio = data[columnIndices.get("Municipio")].trim();
        String localidad = data[columnIndices.get("Localidad")].trim();
        String codigoPostal = data[columnIndices.get("Código postal")].trim();
        String direccion = data[columnIndices.get("Dirección")].trim();
        String margen = data[columnIndices.get("Margen")].trim();
        String tomaDeDatos = data[columnIndices.get("Toma de datos")].trim();
        String tipoEstacion = data[columnIndices.get("Tipo estación")].trim();
        String rotulo = data[columnIndices.get("Rótulo")].trim();
        String horarioDescripcion = data[columnIndices.get("Horario")].trim();

        // Obtener y convertir las coordenadas
        double longitud = parseDouble(data[columnIndices.get("Longitud")].trim());
        double latitud = parseDouble(data[columnIndices.get("Latitud")].trim());

        // Construir la representación WKT de la ubicación
        // Usamos Locale.US para asegurar puntos como separadores decimales
        String ubicacionWKT = String.format(Locale.US,"POINT(%f %f)", longitud, latitud);

        // Precios de combustibles
        Map<String, String> preciosCombustibles = new HashMap<>();
        for (String header : headers) {
            if (header.startsWith("Precio")) {
                preciosCombustibles.put(header, data[columnIndices.get(header)].trim());
            }
        }

        // Insertar o actualizar registros en la base de datos
        int provinciaId = getOrInsertProvincia(provincia);
        int municipioId = getOrInsertMunicipio(municipio, provinciaId);
        int localidadId = getOrInsertLocalidad(localidad, municipioId);
        int codigoPostalId = getOrInsertCodigoPostal(codigoPostal, localidadId);
        int margenId = getOrInsertMargen(margen);
        int horarioId = getOrInsertHorario(horarioDescripcion);
        int marcaId = getOrInsertMarca(rotulo);
        int tipoEstacionId = getOrInsertTipoEstacion(tipoEstacion);
        int estacionId = getOrInsertEstacionServicio(
                direccion, margenId, ubicacionWKT, codigoPostalId, marcaId, horarioId, tipoEstacionId);

        // Procesar precios de combustibles
        processPrecios(estacionId, tomaDeDatos, preciosCombustibles);

        log.debug("Estación procesada con ID: {}", estacionId);
    }

    // Métodos getOrInsert con cacheo de IDs y reutilización de PreparedStatement

    /**
     * Obtiene o inserta una provincia en la base de datos y devuelve su ID.
     *
     * @param nombreProvincia Nombre de la provincia.
     * @return ID de la provincia.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertProvincia(String nombreProvincia) throws SQLException {
        // Verificar si la provincia ya está en caché
        if (provinciaCache.containsKey(nombreProvincia)) {
            int provinciaId = provinciaCache.get(nombreProvincia);
            log.debug("Provincia encontrada en caché: {} con ID {}", nombreProvincia, provinciaId);
            return provinciaId;
        }

        // Intentar obtener el ID de la provincia desde la base de datos
        selectProvinciaStmt.setString(1, nombreProvincia);
        ResultSet rs = selectProvinciaStmt.executeQuery();
        int provinciaId;

        if (rs.next()) {
            // Provincia encontrada
            provinciaId = rs.getInt("provincia_id");
            log.debug("Provincia encontrada en BD: {} con ID {}", nombreProvincia, provinciaId);
        } else {
            // Insertar nueva provincia si no existe
            insertProvinciaStmt.setString(1, nombreProvincia);
            insertProvinciaStmt.executeUpdate();
            ResultSet keys = insertProvinciaStmt.getGeneratedKeys();
            if (keys.next()) {
                provinciaId = keys.getInt(1);
                log.debug("Provincia insertada: {} con ID {}", nombreProvincia, provinciaId);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para la provincia: " + nombreProvincia);
            }
        }

        // Cachear el ID de la provincia para evitar consultas redundantes
        provinciaCache.put(nombreProvincia, provinciaId);
        return provinciaId;
    }

    /**
     * Obtiene o inserta un municipio en la base de datos y devuelve su ID.
     *
     * @param nombreMunicipio Nombre del municipio.
     * @param provinciaId     ID de la provincia asociada.
     * @return ID del municipio.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertMunicipio(String nombreMunicipio, int provinciaId) throws SQLException {
        String key = nombreMunicipio + "_" + provinciaId;
        if (municipioCache.containsKey(key)) {
            return municipioCache.get(key);
        }

        selectMunicipioStmt.setString(1, nombreMunicipio);
        selectMunicipioStmt.setInt(2, provinciaId);
        ResultSet rs = selectMunicipioStmt.executeQuery();
        int municipioId;
        if (rs.next()) {
            municipioId = rs.getInt("municipio_id");
        } else {
            insertMunicipioStmt.setString(1, nombreMunicipio);
            insertMunicipioStmt.setInt(2, provinciaId);
            insertMunicipioStmt.executeUpdate();
            ResultSet keys = insertMunicipioStmt.getGeneratedKeys();
            if (keys.next()) {
                municipioId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para el municipio: " + nombreMunicipio);
            }
        }
        municipioCache.put(key, municipioId);
        return municipioId;
    }

    /**
     * Obtiene o inserta una localidad en la base de datos y devuelve su ID.
     *
     * @param nombreLocalidad Nombre de la localidad.
     * @param municipioId     ID del municipio asociado.
     * @return ID de la localidad.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertLocalidad(String nombreLocalidad, int municipioId) throws SQLException {
        String key = nombreLocalidad + "_" + municipioId;
        if (localidadCache.containsKey(key)) {
            return localidadCache.get(key);
        }

        selectLocalidadStmt.setString(1, nombreLocalidad);
        selectLocalidadStmt.setInt(2, municipioId);
        ResultSet rs = selectLocalidadStmt.executeQuery();
        int localidadId;
        if (rs.next()) {
            localidadId = rs.getInt("localidad_id");
        } else {
            insertLocalidadStmt.setString(1, nombreLocalidad);
            insertLocalidadStmt.setInt(2, municipioId);
            insertLocalidadStmt.executeUpdate();
            ResultSet keys = insertLocalidadStmt.getGeneratedKeys();
            if (keys.next()) {
                localidadId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para la localidad: " + nombreLocalidad);
            }
        }
        localidadCache.put(key, localidadId);
        return localidadId;
    }

    /**
     * Obtiene o inserta un código postal en la base de datos y devuelve su ID.
     *
     * @param codigoPostal Código postal.
     * @param localidadId  ID de la localidad asociada.
     * @return ID del código postal.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertCodigoPostal(String codigoPostal, int localidadId) throws SQLException {
        String key = codigoPostal + "_" + localidadId;
        if (codigoPostalCache.containsKey(key)) {
            return codigoPostalCache.get(key);
        }

        selectCodigoPostalStmt.setString(1, codigoPostal);
        selectCodigoPostalStmt.setInt(2, localidadId);
        ResultSet rs = selectCodigoPostalStmt.executeQuery();
        int codigoPostalId;
        if (rs.next()) {
            codigoPostalId = rs.getInt("codigo_postal_id");
        } else {
            insertCodigoPostalStmt.setString(1, codigoPostal);
            insertCodigoPostalStmt.setInt(2, localidadId);
            insertCodigoPostalStmt.executeUpdate();
            ResultSet keys = insertCodigoPostalStmt.getGeneratedKeys();
            if (keys.next()) {
                codigoPostalId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para el código postal: " + codigoPostal);
            }
        }
        codigoPostalCache.put(key, codigoPostalId);
        return codigoPostalId;
    }

    /**
     * Obtiene o inserta un margen en la base de datos y devuelve su ID.
     *
     * @param nombreMargen Nombre del margen.
     * @return ID del margen.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertMargen(String nombreMargen) throws SQLException {
        if (margenCache.containsKey(nombreMargen)) {
            return margenCache.get(nombreMargen);
        }

        selectMargenStmt.setString(1, nombreMargen);
        ResultSet rs = selectMargenStmt.executeQuery();
        int margenId;
        if (rs.next()) {
            margenId = rs.getInt("margen_id");
        } else {
            insertMargenStmt.setString(1, nombreMargen);
            insertMargenStmt.executeUpdate();
            ResultSet keys = insertMargenStmt.getGeneratedKeys();
            if (keys.next()) {
                margenId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para el margen: " + nombreMargen);
            }
        }
        margenCache.put(nombreMargen, margenId);
        return margenId;
    }

    /**
     * Obtiene o inserta un horario en la base de datos y devuelve su ID.
     *
     * @param descripcionHorario Descripción del horario.
     * @return ID del horario.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertHorario(String descripcionHorario) throws SQLException {
        if (horarioCache.containsKey(descripcionHorario)) {
            return horarioCache.get(descripcionHorario);
        }

        selectHorarioStmt.setString(1, descripcionHorario);
        ResultSet rs = selectHorarioStmt.executeQuery();
        int horarioId;
        if (rs.next()) {
            horarioId = rs.getInt("horario_id");
        } else {
            insertHorarioStmt.setString(1, descripcionHorario);
            insertHorarioStmt.executeUpdate();
            ResultSet keys = insertHorarioStmt.getGeneratedKeys();
            if (keys.next()) {
                horarioId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para el horario: " + descripcionHorario);
            }
        }
        horarioCache.put(descripcionHorario, horarioId);
        return horarioId;
    }

    /**
     * Obtiene o inserta una marca en la base de datos y devuelve su ID.
     *
     * @param nombreMarca Nombre de la marca.
     * @return ID de la marca.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertMarca(String nombreMarca) throws SQLException {
        if (marcaCache.containsKey(nombreMarca)) {
            return marcaCache.get(nombreMarca);
        }

        selectMarcaStmt.setString(1, nombreMarca);
        ResultSet rs = selectMarcaStmt.executeQuery();
        int marcaId;
        if (rs.next()) {
            marcaId = rs.getInt("marca_id");
        } else {
            insertMarcaStmt.setString(1, nombreMarca);
            insertMarcaStmt.executeUpdate();
            ResultSet keys = insertMarcaStmt.getGeneratedKeys();
            if (keys.next()) {
                marcaId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para la marca: " + nombreMarca);
            }
        }
        marcaCache.put(nombreMarca, marcaId);
        return marcaId;
    }

    /**
     * Obtiene o inserta un tipo de estación en la base de datos y devuelve su ID.
     *
     * @param tipoEstacion Tipo de estación.
     * @return ID del tipo de estación.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertTipoEstacion(String tipoEstacion) throws SQLException {
        if (tipoEstacionCache.containsKey(tipoEstacion)) {
            return tipoEstacionCache.get(tipoEstacion);
        }

        selectTipoEstacionStmt.setString(1, tipoEstacion);
        ResultSet rs = selectTipoEstacionStmt.executeQuery();
        int tipoEstacionId;
        if (rs.next()) {
            tipoEstacionId = rs.getInt("tipo_estacion_id");
        } else {
            insertTipoEstacionStmt.setString(1, tipoEstacion);
            insertTipoEstacionStmt.executeUpdate();
            ResultSet keys = insertTipoEstacionStmt.getGeneratedKeys();
            if (keys.next()) {
                tipoEstacionId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para el tipo de estación: " + tipoEstacion);
            }
        }
        tipoEstacionCache.put(tipoEstacion, tipoEstacionId);
        return tipoEstacionId;
    }

    /**
     * Obtiene o inserta una estación de servicio en la base de datos y devuelve su ID.
     *
     * @param direccion      Dirección de la estación.
     * @param margenId       ID del margen.
     * @param ubicacionWKT   Representación WKT de la ubicación.
     * @param codigoPostalId ID del código postal.
     * @param marcaId        ID de la marca.
     * @param horarioId      ID del horario.
     * @param tipoEstacionId ID del tipo de estación.
     * @return ID de la estación de servicio.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertEstacionServicio(
            String direccion, int margenId, String ubicacionWKT, int codigoPostalId, int marcaId,
            int horarioId, int tipoEstacionId) throws SQLException {

        String key = ubicacionWKT;

        if (estacionServicioCache.containsKey(key)) {
            return estacionServicioCache.get(key);
        }

        // Usar la representación WKT para la comparación espacial
        selectEstacionServicioStmt.setString(1, ubicacionWKT);
        ResultSet rs = selectEstacionServicioStmt.executeQuery();
        int estacionId;

        if (rs.next()) {
            // Ya existe, retornar ID
            estacionId = rs.getInt("estacion_id");
        } else {
            // Insertar nueva estación
            insertEstacionServicioStmt.setString(1, direccion);
            insertEstacionServicioStmt.setInt(2, margenId);
            insertEstacionServicioStmt.setString(3, ubicacionWKT);
            insertEstacionServicioStmt.setInt(4, codigoPostalId);
            insertEstacionServicioStmt.setInt(5, marcaId);
            insertEstacionServicioStmt.setInt(6, horarioId);
            insertEstacionServicioStmt.setInt(7, tipoEstacionId);

            insertEstacionServicioStmt.executeUpdate();
            ResultSet keys = insertEstacionServicioStmt.getGeneratedKeys();
            if (keys.next()) {
                estacionId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para la estación de servicio.");
            }
        }
        estacionServicioCache.put(key, estacionId);
        return estacionId;
    }

    /**
     * Procesa los precios de combustibles para una estación y fecha específica.
     *
     * @param estacionId          ID de la estación de servicio.
     * @param tomaDeDatos         Fecha y hora de la toma de datos.
     * @param preciosCombustibles Mapa con los nombres y precios de combustibles.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static void processPrecios(int estacionId, String tomaDeDatos, Map<String, String> preciosCombustibles) throws SQLException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime fechaHora = LocalDateTime.parse(tomaDeDatos, formatter);

        for (Map.Entry<String, String> entry : preciosCombustibles.entrySet()) {
            String combustibleName = entry.getKey().replace("Precio ", "").trim();
            String precioStr = entry.getValue();
            if (precioStr != null && !precioStr.isEmpty()) {
                double precio = parseDouble(precioStr.replace(",", "."));

                int combustibleId = getOrInsertCombustible(combustibleName);

                // Verificar si ya existe un registro con la misma fecha_hora
                selectPrecioCombustibleStmt.setInt(1, estacionId);
                selectPrecioCombustibleStmt.setInt(2, combustibleId);
                selectPrecioCombustibleStmt.setTimestamp(3, Timestamp.valueOf(fechaHora));
                ResultSet rs = selectPrecioCombustibleStmt.executeQuery();
                if (!rs.next()) {
                    // Añadir al batch insert
                    insertPrecioCombustibleStmt.setInt(1, estacionId);
                    insertPrecioCombustibleStmt.setInt(2, combustibleId);
                    insertPrecioCombustibleStmt.setDouble(3, precio);
                    insertPrecioCombustibleStmt.setTimestamp(4, Timestamp.valueOf(fechaHora));
                    insertPrecioCombustibleStmt.addBatch();
                }
            }
        }
    }

    /**
     * Obtiene o inserta un tipo de combustible en la base de datos y devuelve su ID.
     *
     * @param nombreCombustible Nombre del combustible.
     * @return ID del combustible.
     * @throws SQLException Si ocurre un error en la base de datos.
     */
    private static int getOrInsertCombustible(String nombreCombustible) throws SQLException {
        if (combustibleCache.containsKey(nombreCombustible)) {
            return combustibleCache.get(nombreCombustible);
        }

        selectCombustibleStmt.setString(1, nombreCombustible);
        ResultSet rs = selectCombustibleStmt.executeQuery();
        int combustibleId;
        if (rs.next()) {
            combustibleId = rs.getInt("combustible_id");
        } else {
            insertCombustibleStmt.setString(1, nombreCombustible);
            insertCombustibleStmt.executeUpdate();
            ResultSet keys = insertCombustibleStmt.getGeneratedKeys();
            if (keys.next()) {
                combustibleId = keys.getInt(1);
            } else {
                throw new SQLException("No se pudo obtener el ID generado para el combustible: " + nombreCombustible);
            }
        }
        combustibleCache.put(nombreCombustible, combustibleId);
        return combustibleId;
    }

    /**
     * Convierte una cadena a double, manejando posibles excepciones.
     *
     * @param str Cadena a convertir.
     * @return Valor double de la cadena, o 0.0 si la cadena es nula o vacía.
     */
    private static double parseDouble(String str) {
        if (str == null || str.isEmpty()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(str.replace(",", "."));
        } catch (NumberFormatException e) {
            log.warn("No se pudo parsear '{}' a double. Se usará 0.0 por defecto.", str);
            return 0.0;
        }
    }
}
