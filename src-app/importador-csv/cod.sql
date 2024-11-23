-- Crear la base de datos con el juego de caracteres y la intercalación especificados
CREATE DATABASE IF NOT EXISTS estaciones_servicio
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE estaciones_servicio;

-- Tabla: horario
CREATE TABLE horario (
                         horario_id          INT AUTO_INCREMENT PRIMARY KEY,
                         descripcion_horario VARCHAR(255) NOT NULL,
                         detalles            VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla: marca
CREATE TABLE marca (
                       marca_id     INT AUTO_INCREMENT PRIMARY KEY,
                       nombre_marca VARCHAR(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla: margen
CREATE TABLE margen (
                        margen_id     INT AUTO_INCREMENT PRIMARY KEY,
                        nombre_margen VARCHAR(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla: provincia
CREATE TABLE provincia (
                           provincia_id     INT AUTO_INCREMENT PRIMARY KEY,
                           nombre_provincia VARCHAR(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla: municipio
CREATE TABLE municipio (
                           municipio_id     INT AUTO_INCREMENT PRIMARY KEY,
                           nombre_municipio VARCHAR(100) NOT NULL,
                           provincia_id     INT NOT NULL,
                           CONSTRAINT municipio_ibfk_1
                               FOREIGN KEY (provincia_id) REFERENCES provincia (provincia_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla: localidad
CREATE TABLE localidad (
                           localidad_id     INT AUTO_INCREMENT PRIMARY KEY,
                           nombre_localidad VARCHAR(100) NOT NULL,
                           municipio_id     INT NOT NULL,
                           CONSTRAINT localidad_ibfk_1
                               FOREIGN KEY (municipio_id) REFERENCES municipio (municipio_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla: codigo_postal
CREATE TABLE codigo_postal (
                               codigo_postal_id INT AUTO_INCREMENT PRIMARY KEY,
                               codigo_postal    VARCHAR(10) NOT NULL,
                               localidad_id     INT NOT NULL,
                               CONSTRAINT codigo_postal_ibfk_1
                                   FOREIGN KEY (localidad_id) REFERENCES localidad (localidad_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Índices para mejorar el rendimiento en consultas
CREATE INDEX idx_localidad_id ON codigo_postal (localidad_id);
CREATE INDEX idx_municipio_id ON localidad (municipio_id);
CREATE INDEX idx_provincia_id ON municipio (provincia_id);

-- Tabla: tipo_combustible
CREATE TABLE tipo_combustible (
                                  combustible_id     INT AUTO_INCREMENT PRIMARY KEY,
                                  nombre_combustible VARCHAR(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla: tipo_estacion
CREATE TABLE tipo_estacion (
                               tipo_estacion_id INT AUTO_INCREMENT PRIMARY KEY,
                               tipo_estacion    VARCHAR(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabla: estacion_servicio
CREATE TABLE estacion_servicio (
                                   estacion_id      INT AUTO_INCREMENT PRIMARY KEY,
                                   direccion        VARCHAR(255)  NOT NULL,
                                   margen_id        INT           NOT NULL,
                                   ubicacion        POINT NOT NULL SRID 4326,
                                   codigo_postal_id INT           NOT NULL,
                                   marca_id         INT           NOT NULL,
                                   horario_id       INT           NOT NULL,
                                   tipo_estacion_id INT           NOT NULL,
                                   SPATIAL INDEX    (ubicacion),
                                   CONSTRAINT estacion_servicio_ibfk_1
                                       FOREIGN KEY (margen_id) REFERENCES margen (margen_id),
                                   CONSTRAINT estacion_servicio_ibfk_2
                                       FOREIGN KEY (codigo_postal_id) REFERENCES codigo_postal (codigo_postal_id),
                                   CONSTRAINT estacion_servicio_ibfk_3
                                       FOREIGN KEY (marca_id) REFERENCES marca (marca_id),
                                   CONSTRAINT estacion_servicio_ibfk_4
                                       FOREIGN KEY (horario_id) REFERENCES horario (horario_id),
                                   CONSTRAINT estacion_servicio_ibfk_5
                                       FOREIGN KEY (tipo_estacion_id) REFERENCES tipo_estacion (tipo_estacion_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Índices adicionales para mejorar el rendimiento en consultas
CREATE INDEX idx_codigo_postal_id ON estacion_servicio (codigo_postal_id);
CREATE INDEX idx_horario_id ON estacion_servicio (horario_id);
CREATE INDEX idx_marca_id ON estacion_servicio (marca_id);
CREATE INDEX idx_margen_id ON estacion_servicio (margen_id);
CREATE INDEX idx_tipo_estacion_id ON estacion_servicio (tipo_estacion_id);

-- Tabla: precio_combustible
CREATE TABLE precio_combustible (
                                    precio_id      INT AUTO_INCREMENT PRIMARY KEY,
                                    estacion_id    INT           NOT NULL,
                                    combustible_id INT           NOT NULL,
                                    precio         DECIMAL(5,3)  NOT NULL,
                                    fecha_hora     DATETIME      NOT NULL,
                                    CONSTRAINT precio_combustible_ibfk_1
                                        FOREIGN KEY (estacion_id) REFERENCES estacion_servicio (estacion_id),
                                    CONSTRAINT precio_combustible_ibfk_2
                                        FOREIGN KEY (combustible_id) REFERENCES tipo_combustible (combustible_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Índices para la tabla precio_combustible
CREATE INDEX idx_combustible_id ON precio_combustible (combustible_id);
CREATE INDEX idx_estacion_id ON precio_combustible (estacion_id);
