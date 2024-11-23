create table estaciones_servicio.estacion_servicio
(
    estacion_id      int auto_increment
        primary key,
    direccion        varchar(255) not null,
    margen_id        int          not null,
    ubicacion        point        not null,
    codigo_postal_id int          not null,
    marca_id         int          not null,
    horario_id       int          not null,
    tipo_estacion_id int          not null,
    constraint estacion_servicio_ibfk_1
        foreign key (margen_id) references estaciones_servicio.margen (margen_id),
    constraint estacion_servicio_ibfk_2
        foreign key (codigo_postal_id) references estaciones_servicio.codigo_postal (codigo_postal_id),
    constraint estacion_servicio_ibfk_3
        foreign key (marca_id) references estaciones_servicio.marca (marca_id),
    constraint estacion_servicio_ibfk_4
        foreign key (horario_id) references estaciones_servicio.horario (horario_id),
    constraint estacion_servicio_ibfk_5
        foreign key (tipo_estacion_id) references estaciones_servicio.tipo_estacion (tipo_estacion_id)
);

create index idx_codigo_postal_id
    on estaciones_servicio.estacion_servicio (codigo_postal_id);

create index idx_horario_id
    on estaciones_servicio.estacion_servicio (horario_id);

create index idx_marca_id
    on estaciones_servicio.estacion_servicio (marca_id);

create index idx_margen_id
    on estaciones_servicio.estacion_servicio (margen_id);

create index idx_tipo_estacion_id
    on estaciones_servicio.estacion_servicio (tipo_estacion_id);

create spatial index ubicacion
    on estaciones_servicio.estacion_servicio (ubicacion);

