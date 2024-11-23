create table estaciones_servicio.horario
(
    horario_id          int auto_increment
        primary key,
    descripcion_horario varchar(255) not null,
    detalles            varchar(255) null
);

