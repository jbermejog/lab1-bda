create table estaciones_servicio.localidad
(
    localidad_id     int auto_increment
        primary key,
    nombre_localidad varchar(100) not null,
    municipio_id     int          not null,
    constraint localidad_ibfk_1
        foreign key (municipio_id) references estaciones_servicio.municipio (municipio_id)
);

create index idx_municipio_id
    on estaciones_servicio.localidad (municipio_id);

