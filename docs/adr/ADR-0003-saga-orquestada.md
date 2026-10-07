# ADR-0003: SAGA orquestada dentro del Document Service

- **Estado:** Aceptada
- **Fecha:** 2026-10-04
- **Decisor:** P2 (doc-service)
- **Revisada por:** P1 (plataforma), P3 (worker), P4 (QA/integración)
- **Contexto:** SPEC §4, §6; Planificacion S0-P2-06

---

## Contexto

El flujo de procesamiento de un PDF atraviesa múltiples servicios:

1. **doc-service**: crea el documento, firma URL prefirmada, encola trabajo.
2. **MinIO**: almacena el PDF crudo y el `.txt` extraído.
3. **Redis Streams**: transporta el mensaje de trabajo entre doc-service y worker.
4. **extraction-worker**: descarga el PDF, extrae texto, sube el `.txt`, escribe estado.

Cada paso puede fallar de forma independiente (timeout, red caída, datos corruptos).
Si el paso 3 falla después de que el paso 2 subió el PDF, queda un objeto huérfano
en MinIO. Si el paso 4 falla después de subir el `.txt`, queda un `.txt` sin
documento COMPLETED.

Necesitamos una forma de garantizar consistencia entre estos servicios sin
bloquear el path crítico con transacciones distribuidas (2PC no es viable:
MinIO no soporta transacciones, Redis Streams es fire-and-forget).

---

## Decisión

Implementamos una **SAGA orquestada** dentro del doc-service.

- El doc-service es el **orquestador**: conoce todos los pasos, sus compensaciones
  y el estado actual del flujo.
- Cada paso es **asíncrono** y se comunica mediante Redis Streams.
- Si un paso falla de forma **no transitoria**, el orquestador dispara la
  **compensación** (cleanup) de los pasos anteriores.
- La **máquina de estados** del documento (`Document.Status`) es el registro
  durable del progreso de la SAGA.

### Por qué orquestación y no coreografía

| Criterio | Orquestación (elegida) | Coreografía |
|---|---|---|
| Conocimiento del flujo | Centralizado en doc-service | Distribuido en cada servicio |
| Compensación | El orquestador dispara el cleanup | Cada servicio escucha eventos y reacciona |
| debugging | Un solo lugar para trazar el estado | Hay que correlacionar logs de N servicios |
| Acoplamiento | doc-service conoce al worker (aceptado en ADR-0006) | Cada servicio solo conoce el stream |
| Cambios de flujo | Modificar el orquestador | Modificar todos los participantes |

La coreografía es más desacoplada, pero el caso de uso tiene compensaciones
explícitas (borrar `.txt` huérfano, marcar FAILED) que son más claras de
implementar en un solo lugar.

### Estados de la SAGA (resumen)

```
PENDING_UPLOAD → UPLOADED → QUEUED → PROCESSING → COMPLETED
                                ↓           ↓
                        UPLOAD_EXPIRED   RETRYING → EXTRACTION_FAILED → COMPENSATING → FAILED
                                                    ↓
                                                  REJECTED (terminal, sin reintentos)
```

Cada transición se escribe en Mongo con **filtro condicional** (`status` esperado),
de modo que dos actores nunca escriban el mismo estado a la vez sin detectarlo.

---

## Opciones descartadas

### Opción A: 2PC (Two-Phase Commit) transaccional

- **Por qué se descarta:** MinIO no soporta transacciones. Redis Streams no
  participa en transacciones de Mongo. No hay un coordinator distribuido que
  garantice atomicidad across stores. Además, 2PC bloquea el path crítico
  mientras espera confirmación de todos los participantes.
- **Consecuencia descartada:** Peor latencia, mayor superficie de fallo, y
  de todos modos no sería 100% correcto sin soporte transaccional en MinIO.

### Opción B: Coreografía pura (event-driven sin orquestador)

- **Por qué se descarta:** Las compensaciones (borrar `.txt` huérfano, marcar
  FAILED) requieren conocimiento del estado global de la SAGA. En coreografía,
  cada servicio tendría que mantener su propio estado y decidir cuándo compensar,
  lo que duplica lógica y hace el debugging más difícil.
- **Consecuencia descartada:** Más servicios que mantener sincronizados ante
  cambios de flujo; riesgo de ciclos de compensación si dos servicios
  se compensan mutuamente.

### Opción C: Reintentos infinitos sin compensación

- **Por qué se descarta:** Si el worker falla 5 veces y no hay compensación,
  queda un documento en `EXTRACTION_FAILED` con un `.txt` parcial en MinIO
  y un mensaje colgado en el stream. El reconciliador (Fase 2) tendría que
  limpiar todo, lo que acopla más el reconciliador al flujo de la SAGA.
- **Consecuencia descartada:** Acumulación de basura en MinIO y en Mongo
  que alguien tiene que limpiar manualmente o con un job dedicado.

---

## Consecuencias

### Positivas

- Un solo lugar (doc-service) donde vive la lógica de orquestación y compensación.
- El estado del documento en Mongo es el registro durable del progreso.
- Las transiciones condicionales evitan writes perdidos entre actores.
- El reconciliador (Fase 2) actúa como red de seguridad, no como lógica primaria.

### Negativas / Riesgos

- **Acoplamiento aceptado:** el doc-service conoce al extraction-worker
  (mensaje en Redis, formato del resultado). Esto está registrado en ADR-0006
  (el worker escribe estado terminal en Mongo) como acoplamiento explícito.
- **El orquestador es single point of failure para el flujo:** si doc-service
  está caído, no se procesan nuevos documentos. Mitigación: el reconciliador
  reencola documentos colgados cuando doc-service vuelve.
- **Complejidad de debugging en distribuido:** se mitiga con `correlation_id`
  propagado extremo a extremo (S0-P2-03).

---

## Referencias

- SPEC §4 (máquina de estados), §6 (SAGA y compensación), §11.1 (transiciones condicionales)
- ADR-0006 (worker escribe estado terminal)
- Planificacion: S0-P2-06, S2-P2-04 (tabla de transiciones)
