create table estaciones_servicio.municipio
(
    municipio_id     int auto_increment
        primary key,
    nombre_municipio varchar(100) not null,
    provincia_id     int          not null,
    constraint municipio_ibfk_1
        foreign key (provincia_id) references estaciones_servicio.provincia (provincia_id)
);

create index idx_provincia_id
    on estaciones_servicio.municipio (provincia_id);

