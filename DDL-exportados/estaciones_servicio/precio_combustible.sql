create table estaciones_servicio.precio_combustible
(
    precio_id      int auto_increment
        primary key,
    estacion_id    int           not null,
    combustible_id int           not null,
    precio         decimal(5, 3) not null,
    fecha_hora     datetime      not null,
    constraint precio_combustible_ibfk_1
        foreign key (estacion_id) references estaciones_servicio.estacion_servicio (estacion_id),
    constraint precio_combustible_ibfk_2
        foreign key (combustible_id) references estaciones_servicio.tipo_combustible (combustible_id)
);

create index idx_combustible_id
    on estaciones_servicio.precio_combustible (combustible_id);

create index idx_estacion_id
    on estaciones_servicio.precio_combustible (estacion_id);

