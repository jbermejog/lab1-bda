package com.lab1;

/**
 * Excepción personalizada para manejar errores en la clase Consultas.
 */
public class ConsultasException extends Exception {
    public ConsultasException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
