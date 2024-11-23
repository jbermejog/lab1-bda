create table estaciones_servicio.codigo_postal
(
    codigo_postal_id int auto_increment
        primary key,
    codigo_postal    varchar(10) not null,
    localidad_id     int         not null,
    constraint codigo_postal_ibfk_1
        foreign key (localidad_id) references estaciones_servicio.localidad (localidad_id)
);

create index idx_localidad_id
    on estaciones_servicio.codigo_postal (localidad_id);

